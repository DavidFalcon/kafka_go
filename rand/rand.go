package rand

import (
    "strconv"
    "math/rand"
    "math"
    "time"
)

func RandInit() {
    rand.Seed(time.Now().UnixNano())
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const letterNumberBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789 "
var Continents = [...]string{ "North America", "Asia", "South America", "Europe", "Africa", "Australia" }

func RandStringBytes(start int, end int, input string) string {
    length := rand.Intn(end - start) + start
    b := make([]byte, length)
    for i := range b {
        b[i] = input[rand.Intn(len(letterBytes))]
    }
    return string(b)
}

func RandContinent() string {
    length := rand.Intn(len(Continents))
    return Continents[length]
}

func RandRecord() string {
    return strconv.Itoa( rand.Intn(math.MaxInt32 - math.MinInt32) + math.MinInt32 ) + "," +
           RandStringBytes(10, 15, letterBytes) + "," +
           RandStringBytes(15, 25, letterNumberBytes) + "," +
           RandContinent()

}