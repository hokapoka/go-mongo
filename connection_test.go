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

var limitBatchTests = []struct {
	limit, batchSize, count int
}{
	{0, 0, 200},
	{0, 1, 200},
	{0, 2, 200},
	{0, 3, 200},
	{0, 100, 200},
	{0, 500, 200},
	{1, 0, 1},
	{1, 1, 1},
	{1, 2, 1},
	{1, 3, 1},
	{1, 100, 1},
	{1, 500, 1},
	{10, 0, 10},
	{10, 1, 10},
	{10, 2, 10},
	{10, 3, 10},
	{10, 100, 10},
	{10, 500, 10},
}

func TestBasic(t *testing.T) {
	c, err := Dial("127.0.0.1")
	if err != nil {
		t.Fatal("dial", err)
	}
	defer c.Close()

	var m map[string]interface{}
	err = RunCommand(c, "go-mongo-test", Doc{{"dropDatabase", 1}}, &m)
	if err != nil {
		t.Fatal("drop", err)
	}

	for i := 0; i < 200; i++ {
		err = SafeInsert(c, "go-mongo-test.test", nil, map[string]int{"x": i})
		if err != nil {
			t.Fatal("insert", err)
		}
	}

	for _, tt := range limitBatchTests {
		r, err := c.Find("go-mongo-test.test", Doc{}, &FindOptions{Limit: tt.limit, BatchSize: tt.batchSize})
		if err != nil {
			t.Fatal("find", err)
		}
		count := 0
		for r.HasNext() {
			var m map[string]interface{}
			err = r.Next(&m)
			if err != nil {
				t.Error("limitBatchTest:", tt, "count:", count, "next err:", err)
				break
			}
			count += 1
		}
		if count != tt.count {
			t.Error("limitBatchTest:", tt, "bad count:", count)
		}
	}
}
