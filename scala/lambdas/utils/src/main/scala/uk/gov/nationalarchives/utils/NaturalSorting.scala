package uk.gov.nationalarchives.utils

import scala.util.matching.Regex

object NaturalSorting:

  private val NUMBER: Regex = "([0-9]+)".r

  given Ordering[Array[String]] = (a: Array[String], b: Array[String]) => {
    val l = Math.min(a.length, b.length)
    (0 until l).segmentLength(i => a(i) == b(i)) match {
      case i if i == l =>
        Math.signum((b.length - a.length).toFloat).toInt
      case i => (a(i), b(i)) match {
        case (NUMBER(c), NUMBER(d)) =>
          Math.signum((c.toLong - d.toLong).toFloat).toInt
        case (c, d) =>
          c.compareTo(d)
      }
    }
  }

  def natural(s: String): Array[String] = {
    val replacements = Map('\u00df' -> "ss", '\u017f' -> "s", '\u0292' -> "s").withDefault(s => s.toString)
    import java.text.Normalizer
    Normalizer.normalize(Normalizer.normalize(
        s.trim.toLowerCase,
        Normalizer.Form.NFKC),
        Normalizer.Form.NFD)
      .replaceAll("\\p{InCombiningDiacriticalMarks}", "")
      .replaceAll("^(the|a|an) ", "")
      .flatMap(replacements.apply)
      .split("\\s+|(?=[0-9])(?<=[^0-9])|(?=[^0-9])(?<=[0-9])") //Split by whitespace or by digit and non-digit boundaries
  }
