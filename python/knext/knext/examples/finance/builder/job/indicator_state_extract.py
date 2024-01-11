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

from knext.api.component import (
    CSVReader,
    UserDefinedExtractor,
    KGWriter,
    SPGTypeMapping,
)
from knext.client.model.builder_job import BuilderJob
from nn4k.invoker import NNInvoker

from builder.operator.indicator_extract import IndicatorExtractOp
from builder.operator.event_extract import IndicatorStateExtractOp


class IndicatorStateExtract(BuilderJob):
    def build(self):
        source = CSVReader(
            local_path="builder/job/data/document.csv", columns=["input"], start_row=2
        )

        extract = UserDefinedExtractor(
            extract_op=IndicatorStateExtractOp(
                params={"config": "builder/model/openai_infer.json"}
            ),
        )

        indicator_mapping = (
            SPGTypeMapping(spg_type_name=Finance.Indicator)
            .add_property_mapping("id", Finance.Indicator.id)
            .add_property_mapping("id", Finance.Indicator.name)
        )

        state_mapping = (
            SPGTypeMapping(spg_type_name=Finance.State)
            .add_property_mapping("id2", Finance.State.id)
            .add_relation_mapping("derivedFrom", Finance.State.derivedFrom, Finance.Indicator)
        )
        
        sink = KGWriter()

        return source >> extract >> [indicator_mapping, state_mapping] >> sink
