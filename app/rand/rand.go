package rand

import (
    "math"
    "time"
    "strconv"
    "math/rand"
)

func RandInit() {
    rand.Seed(time.Now().UnixNano())
}

// Mixture of numbers, characters, and space for random values
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const letterNumberBytes = letterBytes + " 0123456789"
var Continents = [...]string{ "North America", "Asia", "South America", "Europe", "Africa", "Australia" }

// Generate random string with size between [start, end] from random letters 'input'
func RandStringBytes(start int, end int, input string) string {
    randLen := rand.Intn(end - start) + start
    b := make([]byte, randLen)
    for i := range b {
        b[i] = input[rand.Intn(len(input))]
    }
    return string(b)
}

// Get random continent
func RandContinent() string {
    pos := rand.Intn(len(Continents))
    return Continents[pos]
}

// Generate random CSV record, delimiter is ','
func RandRecord() string {
    return strconv.Itoa( rand.Intn(math.MaxInt32 - math.MinInt32) + math.MinInt32 ) + "," +
           RandStringBytes(10, 15, letterBytes) + "," +
           RandStringBytes(15, 25, letterNumberBytes) + "," +
           RandContinent()
}