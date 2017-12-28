package com.example.orc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.orc.TypeDescription;

/**
 * Created by yukihirookada on 2017/05/13.
 */
public class OrcSchema {

    public static TypeDescription newSchema(Path json_path) throws IOException {
        Map<String, Object> map = readJsonFile(json_path);
        System.out.println(map);
        for (String key : map.keySet()) {
            if (key.equals("properties")) {
                Object properties = map.get(key);
                Map<String, Map<String, Object>> mprop =
                    (Map<String, Map<String, Object>>) properties;
                for (Map.Entry<String, Map<String, Object>> es : mprop.entrySet()) {
                    if (es.getValue().size() != 2) {
                        Map<String, Object> v = es.getValue();
                        v.get("type"); // array or object
                        System.out.println("Oh... " + es.getKey() + "!");
                    }
                }
            }
//      OrcFileKeyWrapper
        }

        TypeDescription schema = TypeDescription.createStruct();
        schema.addField("field1", TypeDescription.createInt())
            .addField("field2", TypeDescription.createString())
            .addField("field3", TypeDescription.createString())
            .addField("sfield1", TypeDescription.createStruct());
        return schema;
    }

    private static Map<String, Object> readJsonFile(Path json_path) throws IOException {
        BufferedReader br = Files.newBufferedReader(json_path, StandardCharsets.UTF_8);
        // JSON -> Map
        Map<String, Object> map = new LinkedHashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        try {
            map = mapper.readValue(br, new TypeReference<LinkedHashMap<String, Object>>() {
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }
}
