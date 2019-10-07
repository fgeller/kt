package main

import (
	"bufio"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp"
)

var epoch = mustParseTime("2010-01-02T12:00:00+02:00")

var resolveTestData = `
0 100 12:00 +2s +3s 13:00 +2s
1 50 09:00 10:00

all=0:newest
0 100 104
1 50 52

all=0:
0 100 104
1 50 52

all=:newest
0 100 104
1 50 52

0=:
0 100 104

0=100:103
0 100 103

----

`

type resolveTestGroup struct {
	times   map[int32][]time.Time
	offsets map[int32]int64

	tests []resolveTest
}

type resolveTest struct {
	offsets string
	expect  map[int32]resolvedInterval
}

func parseResolveTests(c *qt.C, testStr string) []resolveTestGroup {
	blocks := strings.Split(testStr, "\n----\n")
	groups := make([]resolveTestGroup, len(blocks))
	for i, b := range blocks {
		groups[i] = parseResolveGroup(c, b)
	}
	return groups
}

func TestParseResolveTests(t *testing.T) {
	c := qt.New(t)
	// Sanity-check the parseResolveGroup code.
	gs := parseResolveTests(c, `
0 100 +0 +2s +3s +59m55s +2s
1 50 2001-10-23T01:03:00Z +1h

all=0:newest
0 100 104
1 50 52

all=0:
2 3 5
3 50 52
`[1:])
	c.Assert(gs, qt.CmpEquals(cmp.AllowUnexported(
		resolveTestGroup{},
		resolveTest{},
		resolvedInterval{},
	)), []resolveTestGroup{{
		times: map[int32][]time.Time{
			0: {
				epoch,
				epoch.Add(2 * time.Second),
				epoch.Add(5 * time.Second),
				epoch.Add(time.Hour),
				epoch.Add(time.Hour + 2*time.Second),
			},
			1: {
				mustParseTime("2001-10-23T01:03:00Z"),
				mustParseTime("2001-10-23T02:03:00Z"),
			},
		},
		offsets: map[int32]int64{0: 100, 1: 50},
		tests: []resolveTest{{
			offsets: "all=0:newest",
			expect: map[int32]resolvedInterval{
				0: {100, 104},
				1: {50, 52},
			},
		}, {
			offsets: "all=0:",
			expect: map[int32]resolvedInterval{
				2: {3, 5},
				3: {50, 52},
			},
		}},
	}})
}

func parseResolveGroup(c *qt.C, block string) resolveTestGroup {
	g := resolveTestGroup{
		times:   make(map[int32][]time.Time),
		offsets: make(map[int32]int64),
	}
	scan := bufio.NewScanner(strings.NewReader(block))
	for {
		if !scan.Scan() {
			c.Fatalf("unexpected end of resolve test block")
		}
		pfields := strings.Fields(scan.Text())
		if len(pfields) == 0 {
			break
		}
		if len(pfields) < 2 {
			c.Fatalf("too few fields in line %q", scan.Text())
		}
		partition, err := strconv.Atoi(pfields[0])
		c.Assert(err, qt.Equals, nil)
		startOffset, err := strconv.ParseInt(pfields[1], 10, 64)
		c.Assert(err, qt.Equals, nil)
		g.offsets[int32(partition)] = startOffset
		msgs := pfields[2:]
		times := make([]time.Time, len(msgs))
		t := epoch
		for i, m := range msgs {
			if strings.HasPrefix(m, "+") {
				d, err := time.ParseDuration(m[1:])
				c.Assert(err, qt.Equals, nil)
				t = t.Add(d)
				times[i] = t
				continue
			}
			msgTime, err := time.Parse(time.RFC3339, m)
			c.Assert(err, qt.Equals, nil, qt.Commentf("line %q; field %d of %q", scan.Text(), i, msgs))
			if msgTime.Before(t) && i > 0 {
				c.Fatalf("out of order test messages")
			}
			times[i] = msgTime
			t = msgTime
		}
		g.times[int32(partition)] = times
	}
	for scan.Scan() {
		test := resolveTest{
			offsets: scan.Text(),
			expect:  make(map[int32]resolvedInterval),
		}
		for scan.Scan() {
			fields := strings.Fields(scan.Text())
			if len(fields) == 0 {
				break
			}
			if len(fields) != 3 {
				c.Fatalf("expected three fields in a resolved offset for a partition, got %q", scan.Text())
			}
			partition, err := strconv.Atoi(fields[0])
			c.Assert(err, qt.Equals, nil)
			start, err := strconv.ParseInt(fields[1], 10, 64)
			c.Assert(err, qt.Equals, nil)
			end, err := strconv.ParseInt(fields[2], 10, 64)
			c.Assert(err, qt.Equals, nil)
			test.expect[int32(partition)] = resolvedInterval{
				start: start,
				end:   end,
			}
		}
		g.tests = append(g.tests, test)
	}
	return g
}

//// TestResolver tests the resolver.ResolveOffsets method
//// independently of Kafka itself.
//func TestResolverResolveOffsets(t *testing.T) {
//	c := qt.New(t)
//	testGroups := parseResolveTests(c, resolveTestData)
//	for i, g := range testGroups {
//		c.Run(fmt.Sprintf("group%d", i), func(c *qt.C) {
//			c.Parallel()
//			topic := makeTopic(c)
//			for partition, startOff := range g.offsets {
//				for _, m :=
//		})
//	}
//}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
