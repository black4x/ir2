case class DocItem(docInt: Int, tf: Int)

val ll1 = List(DocItem(1, 2), DocItem(1, 3))

val ll2 = List(DocItem(2, 20), DocItem(2, 30))


def sumDocItemList(l1: List[DocItem], l2: List[DocItem]):List[DocItem] =
  (l1 ++ l2).groupBy(_.docInt).mapValues(_.map(_.tf).sum).toList map Function.tupled((id, tf) => DocItem(id, tf))

val l = sumDocItemList(ll1, ll2)