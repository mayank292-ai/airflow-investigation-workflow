# Official sources and selected versions

Selected host packages, observed in the installed environment:

| Component | Version |
|---|---|
| Python | 3.12.14 |
| Observed validation container Python | 3.12.13 |
| LangChain | 1.4.4 |
| langchain-openai | 1.7.1 |
| LangGraph | 1.2.14 |
| langgraph-checkpoint-sqlite | 3.1.1 |
| Pydantic | 2.14.1 |
| OpenAI SDK | 3.29.0 |
| pytest | 9.1.1 |
| Airflow image/API target | 3.3.1, Python 3.12 |
| PostgreSQL image | 16.10 |
| User-confirmed Docker engine | 29.8.2, Linux/amd64 |
| User-confirmed Docker Desktop | 4.95.0 |

`requirements.lock` records resolved Python package versions and hashes.
`pip check` passed during preparation. Runtime reports record framework versions,
model/settings, prompt version, tokens and timing. Pinned Airflow/PostgreSQL
images were pulled and started in regular user PowerShell; genuine execution
and API/evidence access were verified directly. Validation image execution is
still pending. This executor cannot launch the installed Docker executable.

Official implementation references reviewed during development:

- [LangChain agents and create_agent](https://docs.langchain.com/oss/python/langchain/agents)
- [LangChain middleware](https://docs.langchain.com/oss/python/langchain/middleware/overview)
- [Custom wrap_model_call / ModelResponse middleware](https://docs.langchain.com/oss/python/langchain/middleware/custom)
- [Responses reasoning/message phase guidance](https://developers.openai.com/api/docs/guides/reasoning)
- [Responses message phase reference](https://developers.openai.com/api/reference/resources/responses/methods/create)
- [Structured output](https://docs.langchain.com/oss/python/langchain/structured-output)
- [OpenAI supported structured-output schema constraints](https://developers.openai.com/api/docs/guides/structured-outputs)
- [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence)
- [Deep Agents overview](https://docs.langchain.com/oss/python/deepagents/overview)
- [LangChain OpenAI integration](https://docs.langchain.com/oss/python/integrations/chat/openai)
- [GPT-5.4 mini model/pricing](https://developers.openai.com/api/docs/models/gpt-5.4-mini)
- [Airflow 3.3.1 configuration](https://airflow.apache.org/docs/apache-airflow/3.3.1/configurations-ref.html)
- [Airflow 3.3.1 public API authentication](https://airflow.apache.org/docs/apache-airflow/3.3.1/security/api.html)
- [Airflow 3.3.1 OpenAPI source](https://raw.githubusercontent.com/apache/airflow/3.3.1/airflow-core/src/airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml)
- [Airflow Docker quick start](https://airflow.apache.org/docs/apache-airflow/3.3.1/howto/docker-compose/index.html)
- [Airflow LocalExecutor](https://airflow.apache.org/docs/apache-airflow/3.3.1/core-concepts/executor/local.html)
- [Airflow 3.3.1 parsing DagBag implementation](https://raw.githubusercontent.com/apache/airflow/3.3.1/airflow-core/src/airflow/dag_processing/dagbag.py)
- [Docker Windows prerequisites](https://docs.docker.com/desktop/setup/install/windows-install/)
- [Docker virtualization troubleshooting](https://docs.docker.com/desktop/troubleshoot-and-support/troubleshoot/topics/)
- [HP BIOS virtualization procedure](https://support.hp.com/us-en/document/ish_5637142-5637191-16)
- [Intel i7-8550U VT-x/EPT support](https://www.intel.com/content/www/us/en/products/sku/122589/intel-core-i78550u-processor-8m-cache-up-to-4-00-ghz/specifications.html)

Do not treat a current stable URL as a permanent version pin; the adapter/image
target is explicitly 3.3.1. New model/framework/runtime choices require access,
compatibility and evaluation review. The same model can produce varying results.
