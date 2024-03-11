<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="3.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:tns="https://nationalarchives.gov.uk/ClosureData"
                xmlns:xs="http://www.w3.org/2001/XMLSchema">

    <xsl:output method="xml" indent="yes" encoding="UTF-8"/>

    <xsl:template match="/">
        <div class="XSLTransformDiv">
            <div style="height: 50px">
                <span class="XSLTransformTitle">Closure Data</span>
            </div>
            <table class="XSLTransformTable">
                <tr>
                    <th style="text-align: center" class="MQ KQ">Field</th>
                    <th style="text-align: center" class="MQ KQ">Value</th>
                </tr>
                <xsl:apply-templates select="//tns:ClosurePeriods" />
                <xsl:apply-templates select="//tns:StartDate" />
                <xsl:apply-templates select="//tns:ExemptionCodes" />
            </table>
        </div>
    </xsl:template>

    <xsl:template match="tns:ClosurePeriods">
        <tr>
            <td class="standardFieldName">Closure Periods</td>
            <td class="standardFieldValue"><xsl:value-of select="text()"/></td>
        </tr>
    </xsl:template>

    <xsl:template match="tns:StartDate">
        <tr>
            <td class="standardFieldName">Start Date</td>
            <td class="standardFieldValue"><xsl:value-of select="text()"/></td>
        </tr>
    </xsl:template>

    <xsl:template match="tns:ExemptionCodes">
        <tr>
            <td class="standardFieldName">Exemption Codes</td>
            <td class="standardFieldValue"><xsl:value-of select="text()"/></td>
        </tr>
    </xsl:template>


</xsl:stylesheet>
