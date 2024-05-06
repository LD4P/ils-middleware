<!-- xslt will normalize the Sinopia Instance RDFXML structure to the Alma Instance RDFXML structure  -->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:sinopiabf="http://sinopia.io/vocabulary/bf/"
exclude-result-prefixes="sinopiabf" >
  <xsl:output method="xml" indent="yes"/>

  <xsl:template match="*[local-name()='instanceOf']">
    <xsl:copy>
      <xsl:attribute name="rdf:resource" namespace="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
        <xsl:value-of select=".//rdf:Description/@rdf:about" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"/>
      </xsl:attribute>
    </xsl:copy>
  </xsl:template>

  <xsl:template match="bf:Instance" xmlns:bf="http://id.loc.gov/ontologies/bibframe/" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
    <xsl:text disable-output-escaping="yes">&lt;bf:Instance rdf:about="</xsl:text>
    <xsl:value-of select="@rdf:about"/>
    <xsl:text disable-output-escaping="yes">" xmlns:bf="http://id.loc.gov/ontologies/bibframe/"&gt;</xsl:text>
    <xsl:apply-templates select="node()"/>
    <xsl:text disable-output-escaping="yes">&lt;/bf:Instance&gt;</xsl:text>
  </xsl:template>

  <xsl:template match="@*|node()">
      <xsl:copy>
        <xsl:apply-templates select="@*|node()"/>
      </xsl:copy>
  </xsl:template>

  <xsl:template match="/*" priority="1">
    <bib>
        <record_format>lcbf_instance</record_format>
        <suppress_from_publishing>false</suppress_from_publishing>
        <record>
        <rdf:RDF xmlns:bf="http://id.loc.gov/ontologies/bibframe/" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:sinopia="http://sinopia.io/vocabulary/" xmlns:bflc="http://id.loc.gov/ontologies/bflc/" xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#">
          <xsl:apply-templates select="node()"/>
        </rdf:RDF>
      </record>
    </bib>
  </xsl:template>

  <xsl:template match="sinopiabf:nonfiling">
  <bflc:nonSortNum xmlns:bflc="http://id.loc.gov/ontologies/bflc/">
    <xsl:value-of select="."/>
  </bflc:nonSortNum>
</xsl:template>

<!-- the following will make sure edtf does not have trailing slash if it is present  -->

<xsl:template match="bf:date/@rdf:datatype" xmlns:bf="http://id.loc.gov/ontologies/bibframe/" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
  <xsl:attribute name="rdf:datatype" namespace="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
    <xsl:choose>
      <xsl:when test="contains(., 'http://id.loc.gov/datatypes/edtf/')">
        <xsl:value-of select="concat('http://id.loc.gov/datatypes/edtf', substring-after(., 'http://id.loc.gov/datatypes/edtf/'))"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="."/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:attribute>
</xsl:template>

</xsl:stylesheet>