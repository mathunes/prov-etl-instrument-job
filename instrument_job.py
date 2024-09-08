import xml.etree.ElementTree as ET
import sys
import os
import requests # is necessary install with `pip install requests`
from xml.dom.minidom import parseString
import re
import copy

URL_API = 'http://172.21.70.133:8080/api/v1' # is necessary update the host

def get_xml_root(item_xml_path):
    if os.path.exists(item_xml_path):
        item_xml_content = ET.parse(item_xml_path)
        return item_xml_content.getroot()
    else:
        print("Please, set a valid ITEM (XML) path of the job...")
        sys.exit()

def get_connections(root):
    connections = []
    for connection in root.findall("./connection/[@connectorName='FLOW']"):
        connections.append(connection)
    return connections

def get_nodes(root, connections):
    nodes = []
    for node in root.iter('node'):
        for connection in connections:
            if node.find("./elementParameter/[@name='UNIQUE_NAME']").attrib.get("value") == connection.attrib.get("source") or node.find("./elementParameter/[@name='UNIQUE_NAME']").attrib.get("value") == connection.attrib.get("target"):
                if node not in nodes:
                    nodes.append(node)
    return nodes

def get_dataflow_name(item_xml_path):
    return os.path.basename(item_xml_path).replace('_0.1.item', '')

def get_node_unique_name(node):
    unique_name = node.find("./elementParameter/[@name='UNIQUE_NAME']")
    return unique_name.attrib['value']

def get_columns_from_node(node):
    return node.findall("./metadata/[@connector='FLOW']/column")

def get_attributes_from_columns(columns):
    attributes = []
    for column in columns:
        name = column.attrib.get('name')
        type = "VARCHAR" if column.attrib.get('sourceType') == "VARCHAR2" else column.attrib.get('sourceType')
        attributes.append((name, type))
    return attributes

def create_dataflow(dataflow_name):
    url = f"{URL_API}/dataflows/"
    payload = {
        "name": f"{dataflow_name}",
        "description": f"Fluxo do job {dataflow_name} no Talend."
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        return response.json()["id"]
    else:
        print("Error creating Dataflow: " + response.json()["message"])
        sys.exit()


def create_data_transformation(dataflow_id, data_transformation_name):
    url = f"{URL_API}/dataflows/{dataflow_id}/transformations"
    payload = {
        "name": f"{data_transformation_name}"
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        return response.json()["id"]
    else:
        print("Error creating Data Transformation: " + response.json()["message"])
        sys.exit()

def create_dataset_schema(dataflow_id, transformation_id, name, attributes, relation_type, dataset_id=None):
    # Overwritten attributes for summarization // to remove this row, its necessary update the tJavaFlex
    attributes = [('COLUMN_NAME', 'VARCHAR'), ('DATA_VALUE', 'VARCHAR'), ('COUNT', 'INT')]

    if dataset_id:
        url = f"{URL_API}/dataflows/{dataflow_id}/transformations/{transformation_id}/data-set-schema/{dataset_id}"
        payload = {
            "relationType": f"{relation_type}"
        }
    else:
        url = f"{URL_API}/dataflows/{dataflow_id}/transformations/{transformation_id}/data-set-schema"
        payload = {
            "relationType": f"{relation_type}",
            "dataSetSchema": {
                "attributes": [
                    {"name": f"{attribute[0]}", "type": f"{attribute[1]}"} for attribute in attributes
                ]
            }
        }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        return response.json().get("id")
    else:
        print("Error creating Dataset Schema: " + response.json()["message"])
        sys.exit()
    
def create_tJavaFlex_talend_component(dataflow_id, data_transformation_id, data_transformation_name, dataset_id, columns, attributes, node_position):
    root = ET.Element("node", componentName="tJavaFlex", componentVersion="0.101", offsetLabelX="0", offsetLabelY="0", posX=f"{node_position[0]}", posY=f"{int(node_position[1]) + 150}")

    id = data_transformation_id
    
    code_end_value_1 = "String finishedAt" + str(id) + " = new SimpleDateFormat(\"yyyy-MM-dd'T'HH:mm:ss\").format(new java.util.Date());List<Map<String, Integer>> attributesAndValuesList" + str(id) + " = new ArrayList<>();for (int i = 0; i < listRow" + str(id) + ".size(); i++) {    String[] attributesAndValues" + str(id) + " = listRow" + str(id) + ".get(i).split(\",(?=\\\\w+=\\\\w+)\");    boolean notIsInList" + str(id) + ";    for (int j = 0; j < attributesAndValues" + str(id) + ".length; j++) {        notIsInList" + str(id) + " = true;        for (Map<String, Integer> pair" + str(id) + " : attributesAndValuesList" + str(id) + ") {                if (pair" + str(id) + ".containsKey(attributesAndValues" + str(id) + "[j])) {                pair" + str(id) + ".put(attributesAndValues" + str(id) + "[j], pair" + str(id) + ".get(attributesAndValues" + str(id) + "[j]) + 1);                notIsInList" + str(id) + " = false;                break;            }        }                if (notIsInList" + str(id) + ") {            Map<String, Integer> newMap" + str(id) + " = new HashMap<>();            newMap" + str(id) + ".put(attributesAndValues" + str(id) + "[j], 1);            attributesAndValuesList" + str(id) + ".add(newMap" + str(id) + ");        }    }}/* ================= DATA TRANSFORMATION ================= */StringBuilder jsonBuilder" + str(id) + "_1 = new StringBuilder();jsonBuilder" + str(id) + "_1.append(\"{\");jsonBuilder" + str(id) + "_1.append(\"\\\"executedBy\\\": \\\"\" + currentUser" + str(id) + " + \"\\\",\");jsonBuilder" + str(id) + "_1.append(\"\\\"startedAt\\\": \\\"\" + startedAt" + str(id) + " + \"\\\",\");jsonBuilder" + str(id) + "_1.append(\"\\\"finishedAt\\\": \\\"\" + finishedAt" + str(id) + " + \"\\\",\");jsonBuilder" + str(id) + "_1.append(\"\\\"numberOfInputTuples\\\": \\\"\" + numberOfProcessedTuples" + str(id) + " + \"\\\",\");jsonBuilder" + str(id) + "_1.append(\"\\\"numberOfOutputTuples\\\": \\\"\" + numberOfProcessedTuples" + str(id) + " + \"\\\"\");jsonBuilder" + str(id) + "_1.append(\"}\");String jsonString" + str(id) + " = jsonBuilder" + str(id) + "_1.toString();try {    String urlString" + str(id) + " = \""+ URL_API + "/dataflows/" + str(dataflow_id) + "/transformations/" + str(data_transformation_id) + "\";    URL url" + str(id) + " = new URL(urlString" + str(id) + ");    HttpURLConnection connection" + str(id) + " = (HttpURLConnection) url" + str(id) + ".openConnection();    connection" + str(id) + ".setRequestMethod(\"PUT\");    connection" + str(id) + ".setRequestProperty(\"Content-Type\", \"application/json\");    connection" + str(id) + ".setRequestProperty(\"Accept\", \"application/json\");    connection" + str(id) + ".setDoOutput(true);    if (jsonString" + str(id) + " != null && !jsonString" + str(id) + ".isEmpty()) {        try (OutputStream os" + str(id) + " = connection" + str(id) + ".getOutputStream()) {            byte[] input" + str(id) + " = jsonString" + str(id) + ".getBytes(StandardCharsets.UTF_8);            os" + str(id) + ".write(input" + str(id) + ", 0, input" + str(id) + ".length);        }    }    try (BufferedReader br" + str(id) + " = new BufferedReader(            new InputStreamReader(connection" + str(id) + ".getInputStream(), StandardCharsets.UTF_8))) {        StringBuilder response" + str(id) + " = new StringBuilder();        String responseLine" + str(id) + ";        while ((responseLine" + str(id) + " = br" + str(id) + ".readLine()) != null) {            response" + str(id) + ".append(responseLine" + str(id) + ".trim());        }    } finally {        connection" + str(id) + ".disconnect();    }} catch (IOException e) {    e.printStackTrace();}"
    code_end_value_2 = "/* ================= DATA SET ================= */ /* ================= DELETE ================= */ try {    String urlString" + str(id) + " = \"" + URL_API + "/dataflows/" + str(dataflow_id) + "/transformations/" + str(data_transformation_id) + "/data-set-schema/" + str(dataset_id) + "/data-set\";    URL url" + str(id) + " = new URL(urlString" + str(id) + ");    HttpURLConnection connection" + str(id) + " = (HttpURLConnection) url" + str(id) + ".openConnection();    connection" + str(id) + ".setRequestMethod(\"DELETE\");    BufferedReader reader" + str(id) + " = new BufferedReader(new InputStreamReader(connection" + str(id) + ".getInputStream()));    String line" + str(id) + ";    StringBuffer response" + str(id) + " = new StringBuffer();    while ((line" + str(id) + " = reader" + str(id) + ".readLine()) != null) {        response" + str(id) + ".append(line" + str(id) + ");    }    reader" + str(id) + ".close();    connection" + str(id) + ".disconnect();} catch (IOException e) {    e.printStackTrace();}/* ================= INSERT ================= */for (int i = 0; i < attributesAndValuesList" + str(id) + ".size(); i++) {    String columnName" + str(id) + " = attributesAndValuesList" + str(id) + ".get(i).toString().split(\"=\")[0].replace(\"{\", \"\");    String dataValue" + str(id) + " = attributesAndValuesList" + str(id) + ".get(i).toString().split(\"=\")[1];    String count" + str(id) + " = attributesAndValuesList" + str(id) + ".get(i).toString().split(\"=\")[2].replace(\"}\", \"\");    StringBuilder jsonBuilder" + str(id) + "_2 = new StringBuilder();    jsonBuilder" + str(id) + "_2.append(\"{\");    jsonBuilder" + str(id) + "_2.append(\"\\\"column_name\\\": \\\"\" + columnName" + str(id) + " + \"\\\",\");    jsonBuilder" + str(id) + "_2.append(\"\\\"data_value\\\": \\\"\" + dataValue" + str(id) + ".replace(\"\\\\\", \"\\\\\\\\\") + \"\\\",\");    jsonBuilder" + str(id) + "_2.append(\"\\\"count\\\": \\\"\" + count" + str(id) + " + \"\\\"\");    jsonBuilder" + str(id) + "_2.append(\"}\");    jsonString" + str(id) + " = jsonBuilder" + str(id) + "_2.toString();    try {        String urlString" + str(id) + " = \"" + URL_API + "/dataflows/" + str(dataflow_id) + "/transformations/" + str(data_transformation_id) + "/data-set-schema/" + str(dataset_id) + "/data-set\";        URL url" + str(id) + " = new URL(urlString" + str(id) + ");        HttpURLConnection connection" + str(id) + " = (HttpURLConnection) url" + str(id) + ".openConnection();        connection" + str(id) + ".setRequestMethod(\"POST\");        connection" + str(id) + ".setRequestProperty(\"Content-Type\", \"application/json\");        connection" + str(id) + ".setDoOutput(true);        if (jsonString" + str(id) + " != null && !jsonString" + str(id) + ".isEmpty()) {            try (OutputStream os" + str(id) + " = connection" + str(id) + ".getOutputStream()) {                byte[] input" + str(id) + " = jsonString" + str(id) + ".getBytes(StandardCharsets.UTF_8);                os" + str(id) + ".write(input" + str(id) + ", 0, input" + str(id) + ".length);            }        }        try (BufferedReader br" + str(id) + " = new BufferedReader(                new InputStreamReader(connection" + str(id) + ".getInputStream(), StandardCharsets.UTF_8))) {            StringBuilder response" + str(id) + " = new StringBuilder();            String responseLine" + str(id) + ";            while ((responseLine" + str(id) + " = br" + str(id) + ".readLine()) != null) {                response" + str(id) + ".append(responseLine" + str(id) + ".trim());            }        } finally {            connection" + str(id) + ".disconnect();        }    } catch (IOException e) {        e.printStackTrace();    }}"
    
    element_parameters = [
        {"field": "TEXT", "name": "UNIQUE_NAME", "value": f"tJavaFlex_{id}", "show": "false"},
        {"field": "CHECK", "name": "DATA_AUTO_PROPAGATE", "value": "true"},
        {"field": "MEMO_JAVA", "name": "CODE_START", "value": f"String currentUser{id} = System.getProperty(\"user.name\");String startedAt{id} = new SimpleDateFormat(\"yyyy-MM-dd'T'HH:mm:ss\").format(new java.util.Date());int numberOfProcessedTuples{id} = 0;List<String> listRow{id} = new ArrayList<String>();"},
        {"field": "MEMO_JAVA", "name": "CODE_MAIN", "value": f"listRow{id}.add(row{id}.toString().split(\"\\\[\")[1].replace(\"]\", \"\"));numberOfProcessedTuples{id} += 1;"},
        {"field": "MEMO_JAVA", "name": "CODE_END", "value": code_end_value_1 + code_end_value_2},
        {"field": "CHECK", "name": "Version_V2_0", "value": "false", "show": "false"},
        {"field": "CHECK", "name": "Version_V3_2", "value": "false", "show": "false"},
        {"field": "CHECK", "name": "Version_V4.0", "value": "true", "show": "false"},
        {"field": "MEMO_IMPORT", "name": "IMPORT", "value": "import java.util.Arrays;import java.util.ArrayList;import java.util.List;import java.util.Map;import java.util.stream.Collectors;import java.util.HashMap;import java.io.*;import java.net.*;import java.nio.charset.StandardCharsets;"},
        {"field": "TEXT", "name": "LABEL", "value": f"{data_transformation_name}_prov"},
        {"field": "TEXT", "name": "CONNECTION_FORMAT", "value": "row"}
    ]

    for params in element_parameters:
        element_param = ET.SubElement(root, "elementParameter", **params)

    metadata = ET.SubElement(root, "metadata", connector="FLOW", name=f"tJavaFlex_{id}")

    for col in columns:
        column = ET.SubElement(metadata, "column")
        for key, value in col.items():
            column.set(key, value)

    tree = ET.ElementTree(root)

    return tree

def add_tJavaFlex_talend_component_in_xml(item_xml_path, root, tJavaFlex_talend_component_list):    
    for tJavaFlex_talend_component in tJavaFlex_talend_component_list:
        root.append(tJavaFlex_talend_component.getroot())

    formatted_xml = ET.tostring(root, encoding='utf-8', method='xml')
    pretty_xml = parseString(formatted_xml).toprettyxml(indent="  ")
    pretty_xml_no_empty_lines = re.sub(r'\n\s*\n', '\n', pretty_xml)

    pretty_xml_no_empty_lines = pretty_xml_no_empty_lines.replace("&amp;#10;", "")

    item_xml_file = open(item_xml_path, 'w')
    item_xml_file.write(pretty_xml_no_empty_lines)
    item_xml_file.close()

def edit_name_node_outputTables(node, id):
    if node.find("./nodeData/outputTables/") is not None:
        node.find("./nodeData/outputTables").attrib["name"] = f'row{id}'

def remove_flow_connection_from_xml(root):
    for connection in root.findall("./connection/[@connectorName='FLOW']"):
        root.remove(connection)
    return root

def add_new_connections_in_xml(item_xml_path, connections, root):
    for connection in connections:
        root.append(connection)

    formatted_xml = ET.tostring(root, encoding='utf-8', method='xml')
    pretty_xml = parseString(formatted_xml).toprettyxml(indent="  ")
    pretty_xml_no_empty_lines = re.sub(r'\n\s*\n', '\n', pretty_xml)

    pretty_xml_no_empty_lines = pretty_xml_no_empty_lines.replace('<?xml version="1.0" ?>', '<?xml version="1.0" encoding="UTF-8"?>')
    pretty_xml_no_empty_lines = pretty_xml_no_empty_lines.replace('<ns0:ProcessType xmlns:ns0="platform:/resource/org.talend.model/model/TalendFile.xsd" xmlns:ns1="http://www.omg.org/XMI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ns1:version="2.0" defaultContext="Default" jobType="Standard">', '<talendfile:ProcessType xmi:version="2.0" xmlns:xmi="http://www.omg.org/XMI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:TalendMapper="http://www.talend.org/mapper" xmlns:talendfile="platform:/resource/org.talend.model/model/TalendFile.xsd" defaultContext="Default" jobType="Standard">')
    pretty_xml_no_empty_lines = pretty_xml_no_empty_lines.replace('</ns0:ProcessType>', '</talendfile:ProcessType>')

    item_xml_file = open(item_xml_path, 'w')
    item_xml_file.write(pretty_xml_no_empty_lines)
    item_xml_file.close()

def sort_tJavaFlex_talend_connections(root, connections, tJavaFlex_talend_component_list):
    new_connections = []

    for index, connection in enumerate(connections):
        connection_source = connection.attrib["source"]
        connection_target = connection.attrib["target"]

        for tJavaFlex_talend_component in tJavaFlex_talend_component_list:
            label = tJavaFlex_talend_component.find("./elementParameter/[@name='LABEL']").attrib['value']
            unique_name = get_node_unique_name(tJavaFlex_talend_component)

            if connection_source in label: 

                connection_copy = copy.deepcopy(connection)

                connection.set('target', unique_name)
                connection_copy.set('source', unique_name)
                connection.set('label', 'row' + unique_name.replace('tJavaFlex_', ''))

                new_connections.append(connection)
                new_connections.append(connection_copy)
                
        if index == len(connections) - 1:
            for tJavaFlex_talend_component in tJavaFlex_talend_component_list:
                label = tJavaFlex_talend_component.find("./elementParameter/[@name='LABEL']").attrib['value']
                unique_name = get_node_unique_name(tJavaFlex_talend_component)

                if connection_target in label: 
                    last_connection = ET.Element("connection", connectorName="FLOW", label=f"row{unique_name.replace('tJavaFlex_', '')}", lineStyle="0", metaname="connection_target", offsetLabelX="0", offsetLabelY="o", source=connection_target, target=unique_name)
                    new_connections.append(last_connection)

    root = remove_flow_connection_from_xml(root)
    add_new_connections_in_xml(item_xml_path, new_connections, root)

def get_node_position(node):
    posX = node.attrib["posX"]
    posY = node.attrib["posY"]
    return [posX, posY]

def main(item_xml_path):
    dataflow_name = get_dataflow_name(item_xml_path)
    dataflow_id = create_dataflow(dataflow_name)

    root = get_xml_root(item_xml_path)

    connections = get_connections(root)
    nodes = get_nodes(root, connections)

    first_node = True
    dataset_id_input = 0
    dataset_id_output = 0

    tJavaFlex_talend_component_list = []

    for node in nodes:
        data_transformation_name = get_node_unique_name(node)

        data_transformation_id = create_data_transformation(dataflow_id, data_transformation_name)

        columns = get_columns_from_node(node)
        
        node_position = get_node_position(node)

        attributes = get_attributes_from_columns(columns)

        if first_node:
            dataset_id_input = create_dataset_schema(dataflow_id, data_transformation_id, "", attributes, "INPUT", dataset_id=None)
            dataset_id_output = create_dataset_schema(dataflow_id, data_transformation_id, "", attributes, "OUTPUT", dataset_id=None)
            first_node = False
        else:
            dataset_id_input = create_dataset_schema(dataflow_id, data_transformation_id, "", attributes, "INPUT", dataset_id_output)
            dataset_id_output = create_dataset_schema(dataflow_id, data_transformation_id, "", attributes, "OUTPUT", dataset_id=None)

        tJavaFlex_talend_component = create_tJavaFlex_talend_component(dataflow_id, data_transformation_id, data_transformation_name, dataset_id_output, columns, attributes, node_position)

        tJavaFlex_talend_component_list.append(tJavaFlex_talend_component)

        edit_name_node_outputTables(node, data_transformation_id)

    add_tJavaFlex_talend_component_in_xml(item_xml_path, root, tJavaFlex_talend_component_list)

    sort_tJavaFlex_talend_connections(root, connections, tJavaFlex_talend_component_list)

    print(f"The {dataflow_name} job was successfully instrumeted!")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        item_xml_path = sys.argv[1]
        main(item_xml_path)
    else:
        print("Please, set a valid ITEM (XML) path of the job...")
        sys.exit()