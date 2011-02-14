package main

import (
    "github.com/garyburd/go-mongo"
    "log"
)

type ExampleDoc struct {
    Id    mongo.ObjectId "_id"
    Title string
    Body  string
}

func main() {

    // Connect to server.

    c, err := mongo.Dial("localhost")
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    // Insert a document.

    id := mongo.NewObjectId()

    err = mongo.SafeInsert(c, "example-db.example-collection", nil,
        &ExampleDoc{Id: id, Title: "Hello", Body: "Mongo is fun."})
    if err != nil {
        log.Fatal(err)
    }

    // Find the document.

    var doc ExampleDoc
    err = mongo.FindOne(c, "example-db.example-collection",
        map[string]interface{}{"_id": id}, nil, &doc)
    if err != nil {
        log.Fatal(err)
    }

    log.Print(doc.Title, doc.Body)
}
