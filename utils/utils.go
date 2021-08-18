package utils

import (
    "os"
    "fmt"
    "strconv"
)

type RecordValue struct {
    Id int32
    Name string
    Address string
    Continent string
    RawDate string
}

func GetInt(str string) int {
    val, err := strconv.Atoi(str)
    if err != nil {
        // handle error
        fmt.Println(err)
        os.Exit(2)
    }
    return val
}

func GetInt32(str string) int32 {
    val, err := strconv.ParseInt(str, 10, 32)
    if err != nil {
        // handle error
        fmt.Println(err)
        os.Exit(2)
    }
    return int32(val)
}
