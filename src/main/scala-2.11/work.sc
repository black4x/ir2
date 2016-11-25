def getTokensFreqMap(query: String, tokens: List[String]): Map[String, Int] = {
  query.split(" ").map(t => (t, tokens.count(word => word == t))).toMap
}

val query = "South African Sanctions"

val docsTokens = List("1", "2", "African")

println(getTokensFreqMap(query, docsTokens))
