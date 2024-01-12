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
from typing import List, Dict
from knext.api.operator import ExtractOp
from knext.api.record import SPGRecord
from nn4k.invoker import NNInvoker


class IndicatorStateExtractOp(ExtractOp):
    def __init__(self, params: Dict[str, str] = None):
        super().__init__(params)
        # Address for LLM service
        self.config = params["config"]
        self.invoker = NNInvoker.from_config(self.config)
        from builder.operator.prompts import (
            IndicatorNERPrompt,
            IndicatorLogicPrompt,
        )

        self.ner_prompt_op = IndicatorNERPrompt()
        self.logic_prompt_op = IndicatorLogicPrompt()

    def generate(self, input_data):
        return self.invoker.remote_inference(input_data)[0]

    def invoke(self, record: Dict[str, str]) -> List[SPGRecord]:
        # Building LLM inputs with IndicatorNERPrompt
        print("Enter IndicatorStateExtractOp===========================")
        ner_input_data = self.ner_prompt_op.build_prompt(record)
        ner_output = self.generate(ner_input_data)
        ner_records = self.ner_prompt_op.parse_response(ner_output)
        print(f"ner_records = {ner_records}")
        ner_content = []
        for item in ner_records:
            ner_content.append(item.get_property("name"))
        print(f"ner_content = {ner_content}")
        record["input"] += f"\nNER: {ner_content}"
        print(f"record = {record}")
        logic_input_data = self.logic_prompt_op.build_prompt(record)
        logic_output = self.generate(logic_input_data)
        logic_records = self.logic_prompt_op.parse_response(logic_output)
        print(f"logic_records = {logic_records}")
        print("Exit IndicatorStateExtractOp===========================")
        return ner_records + logic_records
