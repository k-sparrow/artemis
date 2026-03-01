import json
from typing import Optional, Sequence, Literal, Union, Any

from huggingface_hub import (
    InferenceClient as HFHubInferenceClient,
    AsyncInferenceClient as HFHubAsyncInferenceClient,
)
from huggingface_hub.hf_api import InferenceProviderMapping
from huggingface_hub.inference._providers import PROVIDER_OR_POLICY_T
from huggingface_hub.inference._providers._common import filter_none
from huggingface_hub.inference._providers.hf_inference import HFInferenceTask
from huggingface_hub.inference._common import (
    RequestParameters,
)

__all__ = [
    "InferenceClient",
    "AsyncInferenceClient",
]


class HFInferenceRerankTask(HFInferenceTask):
    def __init__(self):
        super().__init__("text-reranking")

    def _prepare_payload_as_dict(
        self,
        inputs: Any,
        parameters: dict,
        provider_mapping_info: InferenceProviderMapping,
    ) -> Optional[dict]:
        if not isinstance(inputs, dict):
            raise ValueError(
                f"Unexpected input type for task {self.task} (got {type(inputs)}), excepected dict[query, texts]"
            )
        if "query" not in inputs or "texts" not in inputs:
            raise ValueError(
                f"Unexpected input for task {self.task} (got missing 'query' or 'texts')"
            )

        # Parameters are sent at root-level for feature-extraction task
        # See specs: https://github.com/huggingface/huggingface.js/blob/main/packages/tasks/src/tasks/feature-extraction/spec/input.json
        return {
            "query": inputs["query"],
            "texts": inputs["texts"],
            **filter_none(parameters),
        }

    def get_response(
        self,
        response: Union[bytes, dict],
        request_params: Optional[RequestParameters] = None,
    ) -> Any:
        if isinstance(response, bytes):
            return json.loads(response.decode())
        return response


class InferenceClient(HFHubInferenceClient):
    def __init__(
        self,
        model: Optional[str] = None,
        *,
        provider: Optional[PROVIDER_OR_POLICY_T] = None,
        token: Optional[str] = None,
        timeout: Optional[float] = None,
        headers: Optional[dict[str, str]] = None,
        cookies: Optional[dict[str, str]] = None,
        bill_to: Optional[str] = None,
        # OpenAI compatibility
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        super().__init__(
            model,
            provider=provider,
            token=token,
            timeout=timeout,
            headers=headers,
            cookies=cookies,
            bill_to=bill_to,
            base_url=base_url,
            api_key=api_key,
        )

    def text_reranking(
        self,
        texts: Sequence[str],
        query: str,
        *,
        raw_scores: Optional[bool] = False,
        return_text: Optional[bool] = False,
        truncate: Optional[bool] = False,
        truncation_direction: Optional[Literal["Left", "Right"]] = "Right",
    ) -> None:
        model_id = self.model
        provider_helper = HFInferenceRerankTask()
        request_parameters = provider_helper.prepare_request(
            inputs={"texts": texts, "query": query},
            parameters={
                "raw_scores": raw_scores,
                "return_text": return_text,
                "truncate": truncate,
                "truncation_direction": truncation_direction,
            },
            headers=self.headers,
            model=model_id,
            api_key=self.token,
        )
        response = self._inner_post(request_parameters)
        return json.loads(response.decode())


class AsyncInferenceClient(HFHubAsyncInferenceClient):
    def __init__(
        self,
        model: Optional[str] = None,
        *,
        provider: Optional[PROVIDER_OR_POLICY_T] = None,
        token: Optional[str] = None,
        timeout: Optional[float] = None,
        headers: Optional[dict[str, str]] = None,
        cookies: Optional[dict[str, str]] = None,
        bill_to: Optional[str] = None,
        # OpenAI compatibility
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        super().__init__(
            model,
            provider=provider,
            token=token,
            timeout=timeout,
            headers=headers,
            cookies=cookies,
            bill_to=bill_to,
            base_url=base_url,
            api_key=api_key,
        )

    async def text_reranking(
        self,
        texts: Sequence[str],
        query: str,
        *,
        raw_scores: Optional[bool] = False,
        return_text: Optional[bool] = False,
        truncate: Optional[bool] = False,
        truncation_direction: Optional[Literal["Left", "Right"]] = "Right",
    ) -> None:
        model_id = self.model
        provider_helper = HFInferenceRerankTask()
        request_parameters = provider_helper.prepare_request(
            inputs={"texts": texts, "query": query},
            parameters={
                "raw_scores": raw_scores,
                "return_text": return_text,
                "truncate": truncate,
                "truncation_direction": truncation_direction,
            },
            headers=self.headers,
            model=model_id,
            api_key=self.token,
        )
        response = await self._inner_post(request_parameters)
        return json.loads(response.decode())
