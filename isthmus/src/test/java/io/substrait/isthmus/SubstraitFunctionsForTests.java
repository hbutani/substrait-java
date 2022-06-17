package io.substrait.isthmus;

import io.substrait.function.SimpleExtension;

import java.io.ByteArrayInputStream;

public class SubstraitFunctionsForTests {

    private static String YAML = "%YAML 1.2\n" +
            "---\n" +
            "scalar_functions:\n" +
            "  -\n" +
            "    name: \"round\"\n" +
            "    impls:\n" +
            "      - args:\n" +
            "          - value: decimal<P1,S1>\n" +
            "          - value: i32\n" +
            "        # better inference for return_type scale?\n" +
            "        #   if arg_1 is a literal the return_type scale = its value else arg_0.scale\n" +
            "        return: |-\n" +
            "          DECIMAL<P1, S1>\n" +
            "      - args:\n" +
            "          - value: decimal<P1,S1>\n" +
            "        return: |-\n" +
            "          DECIMAL<P1, 0>\n";

    public static SimpleExtension.ExtensionCollection testCollection =
            SimpleExtension.load("/functions_test_extras.yaml", new ByteArrayInputStream(YAML.getBytes()));
}
