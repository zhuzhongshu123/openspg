# -*- coding: utf-8 -*-

# Copyright 2023 Ant Group CO., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.
import requests
from typing import List, Dict
from knext.api.operator import ExtractOp
from knext.api.record import SPGRecord
from nn4k.invoker import NNInvoker


class IndicatorExtractOp(ExtractOp):
    def __init__(self, params: Dict[str, str] = None):
        super().__init__(params)
        # Address for LLM service
        self.url = self.params["url"]
        from builder.operator.prompts import IndicatorNERPrompt

        self.prompt_op = IndicatorNERPrompt()

    def generate(self, input_data, adapter_name):
        # Request LLM service to get the extraction results
        try:
            return self.invoker.remote_inference(input_data)[0]
        except Exception as e:
            print(f"failed to call generate, info: {e}")
            return {}

    def invoke(self, record: Dict[str, str]) -> List[SPGRecord]:
        # Building LLM inputs with IndicatorNERPrompt
        ner_input = self.prompt_op.build_prompt(record)
        ner_output = self.generate(ner_input, "ner")
        record["ner"] = ner_output["output"]
        # Parsing the LLM output with IndicatorNERPrompt to construct SPGRecords
        ner_result = self.prompt_op.parse_response(record["ner"])

        return ner_result
