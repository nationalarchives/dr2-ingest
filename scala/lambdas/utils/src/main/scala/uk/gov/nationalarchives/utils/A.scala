package uk.gov.nationalarchives.utils

import uk.gov.nationalarchives.utils.NaturalSorting.{fromRomanNumeral, natural, given}

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*



object A extends App:

  List(
    "HL_PO_PB_1_1903_3E7ccciii.West Bromwich Corporation Act, c cciii.pdf",
    "HL_PO_PB_1_1903_3E7ccciv.Hove, Worthing and District Tramways Act, c cciv.pdf",
    "HL_PO_PB_1_1903_3E7cccix.Bournemouth Gas and Water Act, c. ccix.pdf",
    "HL_PO_PB_1_1903_3E7cccl.Erith Tramways and Improvement Act, c ccl.pdf",
    "HL_PO_PB_1_1903_3E7cccli.Great Central Railway Act, c ccli.pdf",
    "HL_PO_PB_1_1903_3E7ccclii.Gateshead Corporation Act, c cclii.pdf",
    "HL_PO_PB_1_1903_3E7cccliii.South Yorkshire Joint Railway Act, c ccliii.pdf",
    "HL_PO_PB_1_1903_3E7cccliv.North Eastern Railway Act, c ccliv.pdf",
    "HL_PO_PB_1_1903_3E7ccclix.Coventry Electric Tramways Act, c cclix.pdf",
    "HL_PO_PB_1_1903_3E7ccclv.Sheffield Corporation Act, c cclv.pdf",
    "HL_PO_PB_1_1903_3E7ccclvi.Cork Harbour Act, c cclvi.pdf"
  ).sortBy(natural).foreach(println)

  List("a1", "a10", "a2").sortBy(natural).foreach(println)


  Files.readAllLines(Path.of("/home/sam/rn.txt")).asScala.toList.foreach { line =>
    val split = line.split("=")
    val num = split.head.toInt
    val rn = split(1)
    assert(rn.fromRomanNumeral == num)
  }
