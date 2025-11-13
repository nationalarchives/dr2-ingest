package uk.gov.nationalarchives.utils

import scala.annotation.tailrec
import scala.util.matching.Regex

object NaturalSorting:

  private val INT: Regex = "([0-9]+)".r
  private val ROMAN: Regex = "^(?i)M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}).*".r

  given Ordering[Array[String]] = (a: Array[String], b: Array[String]) => {
    val l = Math.min(a.length, b.length)
    (0 until l).segmentLength(i => a(i) == b(i)) match {
      case i if i == l => 
        Math.signum((b.length - a.length).toFloat).toInt
      case i => (a(i), b(i)) match {
        case (INT(c), INT(d)) =>
          Math.signum((c.toInt - d.toInt).toFloat).toInt
        case (c, d) =>
          print(c, d)
          (c, d) match {
            case (ROMAN(c), ROMAN(d)) =>
              Math.signum((c.fromRomanNumeral - d.fromRomanNumeral).toFloat).toInt
            case _ => c.compareTo(d)
          }
      }
    }
  }

  enum RomanNumeral(val value: Int):
    case M extends RomanNumeral(1000)
    case D extends RomanNumeral(500)
    case C extends RomanNumeral(100)
    case L extends RomanNumeral(50)
    case X extends RomanNumeral(10)
    case V extends RomanNumeral(5)
    case I extends RomanNumeral(1)


  extension (s: String)
    def fromRomanNumeral: Int = {
      @tailrec
      def parse(r: String, total: Int = 0): Int = {
        if r.isEmpty then
          total
        else if r.length == 1 then
          RomanNumeral.valueOf(r.toUpperCase).value + total
        else
          val thisNum = RomanNumeral.valueOf(r.head.toString)
          val nextNum = RomanNumeral.valueOf(r(1).toString)
          if nextNum.value > thisNum.value then
            parse(r.drop(2), total + (nextNum.value - thisNum.value))
          else
            parse(r.drop(1), total + thisNum.value)
      }
      parse(s)
    }

  def natural(s: String): Array[String] = {
    val replacements = Map('\u00df' -> "ss", '\u017f' -> "s", '\u0292' -> "s").withDefault(s => s.toString) // 8
    import java.text.Normalizer
    Normalizer.normalize(Normalizer.normalize(
        s.trim.toLowerCase,
        Normalizer.Form.NFKC), // 7
        Normalizer.Form.NFD).replaceAll("\\p{InCombiningDiacriticalMarks}", "") // 6
      .replaceAll("^(the|a|an) ", "") // 5
      .flatMap(replacements.apply) // 8
      .split(s"\\s+|(?=[0-9])(?<=[^0-9])|(?=[^0-9])(?<=[0-9])") // 1.3, 2 and 4
  }
