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


class Company(BuilderJob):
    def build(self):
        source = CSVReader(
            local_path="builder/job/data/company.csv", columns=["input"], start_row=2
        )

        from knext.api.auto_prompt import REPrompt

        prompt = REPrompt(
            spg_type_name=Finance.Company,
            property_names=[
                Finance.Company.name,
                Finance.Company.orgCertNo,
                Finance.Company.regArea,
                Finance.Company.businessScope,
                Finance.Company.establishDate,
                Finance.Company.legalPerson,
                Finance.Company.regCapital,
            ],
        )

        extract = LLMBasedExtractor(
            llm=LLMInvoker.from_config("builder/model/openai_infer.json"),
            prompt_ops=[prompt],
        )

        mapping = (
            SubGraphMapping(spg_type_name=Finance.Company)
            .add_mapping_field("name", Finance.Company.id)
            .add_mapping_field("name", Finance.Company.name)
            .add_mapping_field("regArea", Finance.Company.regArea)
            .add_mapping_field("businessScope", Finance.Company.businessScope)
            .add_mapping_field("establishDate", Finance.Company.establishDate)
            .add_mapping_field("legalPerson", Finance.Company.legalPerson)
            .add_mapping_field("regCapital", Finance.Company.regCapital)
        )

        sink = KGWriter()

        return source >> extract >> mapping >> sink


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
