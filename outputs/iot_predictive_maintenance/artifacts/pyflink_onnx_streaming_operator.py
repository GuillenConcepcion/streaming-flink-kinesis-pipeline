#!/usr/bin/env python3
"""
=============================================================================
Odysseus In-Stream Predictive Maintenance Scoring Operator (PyFlink + ONNX)
=============================================================================
Loads a pre-trained LightGBM/XGBoost ONNX artifact inside each Flink
TaskManager's open() lifecycle method for sub-millisecond in-stream scoring.
"""

import json
import numpy as np
from pyflink.datastream.functions import RichMapFunction, RuntimeContext


class ONNXPredictiveMaintenanceScoringMap(RichMapFunction):
    """
    Stateful/Stateless RichMapFunction evaluating real-time sliding window
    features against the calibrated ONNX model in TaskManager memory.
    """

    def __init__(self, model_onnx_path: str, optimal_threshold: float = 0.42):
        self.model_onnx_path = model_onnx_path
        self.optimal_threshold = optimal_threshold
        self.ort_session = None
        self.input_name = None

    def open(self, runtime_context: RuntimeContext):
        import onnxruntime as ort
        sess_options = ort.SessionOptions()
        sess_options.intra_op_num_threads = 1
        self.ort_session = ort.InferenceSession(
            self.model_onnx_path,
            sess_options=sess_options,
            providers=["CPUExecutionProvider"],
        )
        self.input_name = self.ort_session.get_inputs()[0].name

    def map(self, event_json: str) -> str:
        payload = json.loads(event_json)
        feature_vector = np.array([payload["features"]], dtype=np.float32)
        ort_outs = self.ort_session.run(None, {self.input_name: feature_vector})
        prob_failure = float(ort_outs[1][0][1]) if len(ort_outs) > 1 else float(ort_outs[0][0])

        payload["failure_probability_score"] = round(prob_failure, 4)
        payload["is_predicted_failure_12h"] = bool(prob_failure >= self.optimal_threshold)
        return json.dumps(payload)
