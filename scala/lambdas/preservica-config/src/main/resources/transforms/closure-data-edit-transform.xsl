<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="3.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:tns="https://nationalarchives.gov.uk/ClosureData"
                xmlns:xs="http://www.w3.org/2001/XMLSchema">

    <xsl:output method="html" encoding="UTF-8"/>

    <!-- Value for this transform name, to select any transform specific text -->
    <xsl:variable name="xslt" select="'tns'"/>

    <xsl:include href="common_editor_templates.xslt"/>

    <xsl:template match="/">
        <div>

            <!-- Put the stylesheet override in here. -->
            <xsl:if test="$cssOverride">
                <link rel="stylesheet" type="text/css">
                    <xsl:attribute name="href">
                        <xsl:value-of select="$cssOverride"/>
                    </xsl:attribute>
                </link>
            </xsl:if>

            <!-- Main Title for page -->
            <span class="XSLTransformTitle">
                <xsl:call-template name="getString">
                    <xsl:with-param name="stringName" select="'mainHeader'"/>
                </xsl:call-template>
            </span>

            <br/>
            <table class="XSLTransformTable">
                <col width="200px" />
                <col width="300px" />
                <xsl:call-template name="add-record-information" />
            </table>
        </div>
    </xsl:template>

    <!-- Template for the Record Information -->
    <xsl:template name="add-record-information">

        <!-- The individual fields -->
        <xsl:call-template name="add-field-node">
            <xsl:with-param name="field">
                <path>//Q{https://nationalarchives.gov.uk/ClosureData}ClosurePeriods</path>
            </xsl:with-param>
            <xsl:with-param name="label">Closure Periods</xsl:with-param>
        </xsl:call-template>
        <xsl:call-template name="add-field-node">
            <xsl:with-param name="field">
                <path>//Q{https://nationalarchives.gov.uk/ClosureData}StartDate</path>
            </xsl:with-param>
            <xsl:with-param name="label">Start Date</xsl:with-param>
            <xsl:with-param name="type">date</xsl:with-param>
        </xsl:call-template>
        <xsl:call-template name="add-field-node">
            <xsl:with-param name="field">
                <path>//Q{https://nationalarchives.gov.uk/ClosureData}ExemptionCodes</path>
            </xsl:with-param>
            <xsl:with-param name="label">Exemption Codes</xsl:with-param>
        </xsl:call-template>
    </xsl:template>
</xsl:stylesheet>
