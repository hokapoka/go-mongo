// Copyright 2011 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package mongo

import (
	"testing"
)

func dialAndDrop(t *testing.T, db string) Conn {
	c, err := Dial("127.0.0.1")
	if err != nil {
		t.Fatal("dial", err)
	}
	err = RunCommand(c, db, Doc{{"dropDatabase", 1}}, nil)
	if err != nil {
		c.Close()
		t.Fatal("drop", err)
	}
	return c
}

var findOptionsTests = []struct {
	limit         int
	batchSize     int
	exhaust       bool
	expectedCount int
}{
	{0, 0, false, 200},
	{0, 1, false, 200},
	{0, 2, false, 200},
	{0, 3, false, 200},
	{0, 100, false, 200},
	{0, 500, false, 200},

	{1, 0, false, 1},
	{1, 1, false, 1},
	{1, 2, false, 1},
	{1, 3, false, 1},
	{1, 100, false, 1},
	{1, 500, false, 1},

	{10, 0, false, 10},
	{10, 1, false, 10},
	{10, 2, false, 10},
	{10, 3, false, 10},
	{10, 100, false, 10},
	{10, 500, false, 10},

	{200, 3, false, 200},
	{200, 3, true, 200},
	{0, 3, true, 200},
}

func TestFindOptions(t *testing.T) {
	c := dialAndDrop(t, "go-mongo-test")
	defer c.Close()

	for i := 0; i < 200; i++ {
		err := SafeInsert(c, "go-mongo-test.test", nil, map[string]int{"x": i})
		if err != nil {
			t.Fatal("insert", err)
		}
	}

	for _, tt := range findOptionsTests {
		r, err := c.Find("go-mongo-test.test", Doc{}, &FindOptions{
			Limit:     tt.limit,
			BatchSize: tt.batchSize,
			Exhaust:   tt.exhaust})
		if err != nil {
			t.Error("find", err)
			continue
		}
		count := 0
		for r.HasNext() {
			var m map[string]interface{}
			err = r.Next(&m)
			if err != nil {
				t.Error("findOptionsTest:", tt, "count:", count, "next err:", err)
				break
			}
			count += 1
		}
		if count != tt.expectedCount {
			t.Error("findOptionsTest:", tt, "bad count:", count)
		}
	}
}

func TestTailableCursor(t *testing.T) {
	c := dialAndDrop(t, "go-mongo-test")
	defer c.Close()

	err := RunCommand(c, "go-mongo-test",
		Doc{{"create", "capped"},
			{"capped", true},
			{"size", 1000.0}},
		nil)
	if err != nil {
		t.Fatal("create capped", err)
	}

	var r Cursor
	for n := 1; n < 4; n++ {
		for i := 0; i < n; i++ {
			err = SafeInsert(c, "go-mongo-test.capped", nil, map[string]int{"x": i})
			if err != nil {
				t.Fatal("insert", i, err)
			}
		}

		if r == nil {
			r, err = c.Find("go-mongo-test.capped", Doc{}, &FindOptions{Tailable: true})
			if err != nil {
				t.Fatal("find", err)
			}
			defer r.Close()
		}

		i := 0
		for r.HasNext() {
			var m map[string]interface{}
			err = r.Next(&m)
			if err != nil {
				t.Fatal("next", n, i, err)
			}
			if m["x"] != int64(i) {
				t.Fatal("expect", i, "actual", m["x"])
			}
			i += 1
		}
		if i != n {
			t.Fatal("count: expect", n, "actual", i)
		}

	}
}
