<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:output method="html" omit-xml-declaration="yes" />

    <xsl:template match="XMLFragment">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="scopecontent">
        <xsl:apply-templates />
    </xsl:template>


    <xsl:template match="bioghist">
        <div>
            <xsl:apply-templates />
        </div>
    </xsl:template>

    <xsl:template match="arrangement">
        <div>
            <xsl:apply-templates />
        </div>
    </xsl:template>

    <xsl:template match="unittitle">
        <div>
            <xsl:apply-templates />
        </div>
    </xsl:template>

    <xsl:template match="archref">
        <xsl:choose>
            <xsl:when test="@href!=''">
                <xsl:value-of select="@href"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:apply-templates />
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template match="extref">
        <xsl:choose>
            <xsl:when test="@href!=''">
                <xsl:value-of select="@href"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:apply-templates />
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template match="ref">
        <xsl:choose>
            <xsl:when test="@href!='' and not(@target)">
                <xsl:value-of select="."/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:apply-templates />
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template match="table">
        <xsl:text>&#xa;</xsl:text>
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="chronlist">
        <table>
            <xsl:for-each select="chronitem">
                <xsl:text>&#xa;</xsl:text>
                <xsl:apply-templates />
            </xsl:for-each>
        </table>
    </xsl:template>

    <xsl:template match="list">
        <xsl:text>&#xa;</xsl:text>
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="list/item">
        <xsl:text>&#xa;</xsl:text>
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="emph">
        <xsl:text>&#xa;</xsl:text>
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="title">
        <xsl:text>&#xa;</xsl:text>
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="lb">
        <xsl:text>&#xa;</xsl:text>
    </xsl:template>

    <xsl:template match="p" name="p">
        <xsl:text>&#xa;</xsl:text>
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="note">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="blockquote">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="abbr">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="abstract">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="accruals">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="acqinfo">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="address">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="addressline">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="add">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="admininfo">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="altformavail">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="appraisal">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="archdesc">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="archdescgrp">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="author">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="bibref">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="bibseries">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="bibliography">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="change">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="chronitem">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="c08">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="c11">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="c05">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="c01">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="c04">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="c09">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="c02">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="c07">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="c06">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="c10">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="c03">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="c12">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="c">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="container">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="controlaccess">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="corpname">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="creation">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="custodhist">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="date">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="unitdate">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="defitem">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="dsc">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="dscgrp">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="did">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="dao">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="daodesc">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="daogrp">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="daoloc">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="dimensions">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="dentry">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="drow">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="eadgrp">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="eadheader">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="eadid">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="edition">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="editionstmt">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="ead">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="event">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="eventgrp">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="expan">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="extptr">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="exptrloc">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="extrefloc">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="extent">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="famname">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="filedesc">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="fileplan">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="head01">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="frontmatter">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="function">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="genreform">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="geogname">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="head">
    </xsl:template>

    <xsl:template match="unitid">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="imprint">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="index">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="indexentry">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="label">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="language">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="langusage">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="scopecontent/language">
        <p>
            <xsl:apply-templates />
        </p>
    </xsl:template>

    <xsl:template match="linkgrp">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="listhead">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="name">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="namegrp">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="notestmt">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="num">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="occupation">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="organization">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="origination">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="odd">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="otherfindaid">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="persname">
        <xsl:choose>
            <xsl:when test="emph">
                <xsl:for-each select="child::emph">
                    <xsl:apply-templates />
                    <xsl:if test="position() != last()">
                        <xsl:text> </xsl:text>
                    </xsl:if>
                </xsl:for-each>
            </xsl:when>
            <xsl:otherwise>
                <xsl:apply-templates/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template match="physdesc">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="physloc">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="ptr">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="ptrgrp">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="ptrloc">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="prefercite">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="processinfo">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="profiledesc">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="publicationstmt">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="publisher">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="reloc">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="relatedmaterial">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="repository">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="accessrestrict">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="userrestrict">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="revisiondesc">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="runner">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="head02">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="seperatedmaterial">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="seriesstmt">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="spanspec">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="sponsor">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="subject">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="subarea">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="subtitle">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="tbody">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="colspec">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="entry">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="tfoot">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="tgroup">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="thead">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="row">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="tspec">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="div">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="titlepage">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="titleproper">
        <xsl:apply-templates />
    </xsl:template>

    <xsl:template match="titletmt">
        <xsl:apply-templates />
    </xsl:template>

</xsl:stylesheet>
