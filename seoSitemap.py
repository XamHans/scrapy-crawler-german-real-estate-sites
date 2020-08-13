from xml.etree import ElementTree, cElementTree
from xml.dom import minidom
from database import DataBase
db = DataBase()

stadtList = db.findAlleStadte()

#  xmlns="https://www.sitemaps.org/schemas/sitemap/0.9" xmlns:news="https://www.google.com/schemas/sitemap-news/0.9"
root = ElementTree.Element('urlset')

for stadt in stadtList:
    stadtName = stadt["Stadt"]
    stadtviertel = stadt["Stadtviertel"]
    stadtName = str(stadtName).strip().replace(' ','')
    if not stadtName:
        continue
    if len(stadtviertel) < 10:
        print('zu wenige viertel, skip')
        continue

    child1 = ElementTree.SubElement(root, 'url')
    child1_1 = ElementTree.SubElement(child1, 'loc')
    child1_1.text = 'https://www.immorobo.de/list/'+stadtName+"/wohnung/mieten/"
    child1_2 = ElementTree.SubElement(child1, 'lastmod')
    child1_2.text = '2020-07-29'
    child1_3 = ElementTree.SubElement(child1, 'changefreq')
    child1_3.text = 'daily'
    child1_4 = ElementTree.SubElement(child1, 'priority')
    child1_4.text = '1'
    
    child2 = ElementTree.SubElement(root, 'url')
    child2_1 = ElementTree.SubElement(child2, 'loc')
    child2_1.text = 'https://www.immorobo.de/list/'+stadtName+"/wohnung/kaufen/"
    child2_2 = ElementTree.SubElement(child2, 'lastmod')
    child2_2.text = '2020-07-29'
    child1_3 = ElementTree.SubElement(child2, 'changefreq')
    child1_3.text = 'daily'
    child1_4 = ElementTree.SubElement(child2, 'priority')
    child1_4.text = '1'
    
    child3 = ElementTree.SubElement(root, 'url')
    child3_1 = ElementTree.SubElement(child3, 'loc')
    child3_1.text = 'https://www.immorobo.de/list/'+stadtName+"/haus/mieten/"
    child3_2 = ElementTree.SubElement(child3, 'lastmod')
    child3_2.text = '2020-07-29'
    child1_3 = ElementTree.SubElement(child3, 'changefreq')
    child1_3.text = 'daily'
    child1_4 = ElementTree.SubElement(child3, 'priority')
    child1_4.text = '1'
    
    child4 = ElementTree.SubElement(root, 'url')
    child4_1 = ElementTree.SubElement(child4, 'loc')
    child4_1.text = 'https://www.immorobo.de/list/'+stadtName+"/haus/kaufen/"
    child4_2 = ElementTree.SubElement(child4, 'lastmod')
    child4_2.text = '2020-07-29'
    child1_3 = ElementTree.SubElement(child4, 'changefreq')
    child1_3.text = 'daily'
    child1_4 = ElementTree.SubElement(child4, 'priority')
    child1_4.text = '1'
    
    child5 = ElementTree.SubElement(root, 'url')
    child5_1 = ElementTree.SubElement(child5, 'loc')
    child5_1.text = 'https://www.immorobo.de/list/'+stadtName+"/wg/mieten/"
    child5_2 = ElementTree.SubElement(child5, 'lastmod')
    child5_2.text = '2020-07-29'
    child1_3 = ElementTree.SubElement(child5, 'changefreq')
    child1_3.text = 'daily'
    child1_4 = ElementTree.SubElement(child5, 'priority')
    child1_4.text = '1'
    
tree = cElementTree.ElementTree(root) # wrap it in an ElementTree instance, and save as XML

t = minidom.parseString(ElementTree.tostring(root)).toprettyxml() # Since ElementTree write() has no pretty printing support, used minidom to beautify the xml.
tree1 = ElementTree.ElementTree(ElementTree.fromstring(t))

tree1.write("sitemap_index.xml",encoding='utf-8', xml_declaration=True)