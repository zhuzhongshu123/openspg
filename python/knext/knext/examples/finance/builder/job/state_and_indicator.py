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


from schema.finance_schema_helper import Finance

from knext.api.component import CSVReader, LLMBasedExtractor, KGWriter, SubGraphMapping
from knext.client.model.builder_job import BuilderJob
from nn4k.invoker import LLMInvoker


class StateAndIndicator(BuilderJob):
    def build(self):
        source = CSVReader(
            local_path="builder/job/data/document.csv", columns=["input"], start_row=2
        )

        from builder.operator.IndicatorNER import IndicatorNER
        from builder.operator.IndicatorREL import IndicatorREL
        from builder.operator.IndicatorLOGIC import (
            IndicatorLOGIC,
        )

        extract = LLMBasedExtractor(
            llm=LLMInvoker.from_config("builder/model/openai_infer.json"),
            prompt_ops=[IndicatorNER(), IndicatorREL(), IndicatorLOGIC()],
        )

        state_mapping = (
            SubGraphMapping(spg_type_name=Finance.State)
            .add_mapping_field("id", Finance.State.id)
            .add_mapping_field("name", Finance.State.name)
            .add_mapping_field("causes", Finance.State.causes)
            .add_predicting_field(Finance.State.derivedFrom)
        )

        indicator_mapping = (
            SubGraphMapping(spg_type_name=Finance.Indicator)
            .add_mapping_field("id", Finance.Indicator.id)
            .add_mapping_field("name", Finance.Indicator.name)
        )

        sink = KGWriter()

        return source >> extract >> [state_mapping, indicator_mapping] >> sink


if __name__ == "__main__":
    from knext.api.auto_prompt import REPrompt

    prompt = REPrompt(
        spg_type_name=Finance.Company,
        property_names=[
            Finance.Company.orgCertNo,
            Finance.Company.regArea,
            Finance.Company.businessScope,
            Finance.Company.establishDate,
            Finance.Company.legalPerson,
            Finance.Company.regCapital,
        ],
    )
    print(prompt.template)
