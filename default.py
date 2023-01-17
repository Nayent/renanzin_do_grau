# VERSION: 0.1.0

import functools
import importlib
import time
import traceback

from airflow.exceptions import AirflowSkipException
from airflow.utils.task_group import TaskGroup
from manager import DAGBuilder, Manager
from operators.python import get_current_context
from operators import CallCalc, UploadStage, WipeStage, LogStart, LogEnd

from config import Config, Environment
from libs.slack import Slack
from logger import log
from tasks.senseconnect import (
    add_or_sub, aggregation, concatenation, conditional, create_column,
    deduplication, filter, get_data, hash_field, load_data, separation,
    truncate, math, merge
)


args = {'dag_name': 'c229_dag_integrao_jira', 'description': 'integração_jira', 'mode': 'upsert', 'groups_dependencies': {'jiratarefas': {'dependency': 'wipe_stage', 'args': {'skip_when_fail': True}}}, 'tasks_dependencies': {'jiratarefas.customizada_344': ['jiratarefas.outras_apis_146'], 'jiratarefas.carregamento_333': ['jiratarefas.concatenao__cnpj_group_332'], 'jiratarefas.separaocnpj_147': ['jiratarefas.customizada_344'], 'jiratarefas.novo_campo_331': ['jiratarefas.separaofields_149'], 'jiratarefas.carregamento_164': ['jiratarefas.separaofields_149'], 'jiratarefas.concatenao__cnpj_group_332': ['jiratarefas.novo_campo_331'], 'jiratarefas.filtro_de_dadoscnpj_no_vazio_148': ['jiratarefas.separaocnpj_147'], 'jiratarefas.separaofields_149': ['jiratarefas.filtro_de_dadoscnpj_no_vazio_148']}, 'task_groups': {'jiratarefas': [{'label': 'Outras APIs [146]', 'task_type': 'get_data', 'description': 'outras_apis_146', 'payload': {'output_file': 'jiratarefas/task_146_outras_apis.csv', 'load_type': 'total', 'source': 'other_apis', 'connection': 'gAAAAABjxX0RjNN2RIGrrdpcN9hjDN5gk9n5u2K055zlbuXsN6F7sfu6uEJ9Fby8grt6sxgm3Zi7pdsclCHfEq15zR-iW4e_TtPiQSo0nrwjJpALAk5JafO1qC6XHZC5SHgK-jq5jLaXSYcdHTz9E0cFTiSwygP7q1rblNP7XJg-IP5qg1FJ2giDR4kDELD5zVWRrfZcNrlds3TBxr9sHwxso0EycClNog==', 'connection_default': False, 'connection_source': 'other_apis', 'file_columns': {'key': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'fields': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}}, 'method': 'GET', 'url': 'https://desenvolvimento.atlassian.net/rest/api/3/search?jql=project=CST OR PROJECT=T0800 OR PROJECT=SHIFT OR PROJECT=BI OR PROJECT=CLD OR PROJECT=SERV OR PROJECT=SIM', 'response_path': '$.issues', 'query_params': {}, 'header_params': {}, 'body_params': {}, 'paginate': {'keys': {'limit': 'maxResults', 'offset': 'startAt'}, 'type': 'offset', 'values': {'limit': '$.maxResults', 'offset': '0'}}}}, {'label': 'Concatenação - CNPJ Group [332]', 'task_type': 'concatenate', 'description': 'concatenao__cnpj_group_332', 'payload': {'output_file': 'jiratarefas/task_332_concatenacao__cnpj_group.csv', 'input_file': 'jiratarefas/task_331_novo_campo.csv', 'mapping': [{'fields': ['Prefixo', 'cnpj'], 'delimiter': '-', 'new_field_name': 'cnpj2'}]}}, {'label': 'Customizada [344]', 'task_type': 'custom_task', 'description': 'customizada_344', 'payload': {'output_file': 'jiratarefas/task_344_customizada.csv', 'input_file': 'jiratarefas/task_146_outras_apis.csv', 'params': {}, 'classname': 'DescriptionContent', 'filename': 'description_content.py', 'output_schema': {'fields': {'key': {'type': 'text'}, 'fields': {'type': 'text'}, 'status_dt_venc': {'type': 'text'}}}}}, {'label': 'Separação:fields [149]', 'task_type': 'separation', 'description': 'separaofields_149', 'payload': {'output_file': 'jiratarefas/task_149_separacaofields.csv', 'input_file': 'jiratarefas/task_148_filtro_de_dadoscnpj_nao_vazio.csv', 'mapping': [{'type': 'json', 'field': 'Data de criação', 'attribute': 'created', 'field_type': {'type': 'date', 'format': '%Y-%m-%d'}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'tipo de solicitação', 'attribute': '$.customfield_12137.value', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'produto', 'attribute': '$.customfield_11886.value', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'complexidade', 'attribute': '$.customfield_11868.value', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'prioridade', 'attribute': '$.priority.name', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'data de aprovação comercial', 'attribute': 'customfield_12096', 'field_type': {'type': 'date', 'format': '%Y-%m-%d'}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'numero sankhya', 'attribute': 'customfield_12138', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'mrr', 'attribute': 'customfield_12244', 'field_type': {'mask': '', 'type': 'number', 'decimal_sep': '.'}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'planejamento de entrega', 'attribute': '$.customfield_12258.value', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'disponível para faturamento', 'attribute': 'customfield_12266', 'field_type': {'type': 'date', 'format': '%Y-%m-%d'}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'motivo de bloqueio', 'attribute': '$.customfield_12179[0].value', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'status', 'attribute': '$.status.name', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'nome projeto', 'attribute': '$.project.name', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'titulo', 'attribute': 'summary', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'contato', 'attribute': 'customfield_11912', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'e-mail', 'attribute': 'customfield_11965', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'entregue ao cliente', 'attribute': 'customfield_11922', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'Descrição', 'attribute': 'description.content', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}, {'type': 'json', 'field': 'Responsável', 'attribute': 'assignee.displayName', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'origin_field': 'fields'}]}}, {'label': 'Novo Campo [331]', 'task_type': 'create_column', 'description': 'novo_campo_331', 'payload': {'output_file': 'jiratarefas/task_331_novo_campo.csv', 'input_file': 'jiratarefas/task_149_separacaofields.csv', 'mapping': [{'equals_to': {'type': 'custom', 'value': 'G'}, 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': False, 'remove_special_char': False, 'remove_dots_and_dash': False}, 'new_field_name': 'Prefixo'}]}}, {'label': 'Carregamento [164]', 'task_type': 'load_data', 'description': 'carregamento_164', 'payload': {'output_file': 'jiratarefas/task_164_carregamento.csv', 'input_file': 'jiratarefas/task_149_separacaofields.csv', 'encoding': 'utf-8-sig', 'delimiter': '|', 'destiny_table': 'custom_data', 'integration_type': 'upsert', 'file_key': ['key'], 'mapping': [{'file_column': 'key', 'sense_column': 'id_task', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'mrr', 'sense_column': 'mrr', 'options': 'overwrite', 'type': 'number'}, {'file_column': 'cnpj', 'sense_column': None, 'options': 'ignore', 'type': 'text'}, {'file_column': 'e-mail', 'sense_column': 'email', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'fields', 'sense_column': None, 'options': 'ignore', 'type': 'text'}, {'file_column': 'status', 'sense_column': 'status', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'titulo', 'sense_column': 'titulo', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'contato', 'sense_column': 'contato', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'produto', 'sense_column': 'produto', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'prioridade', 'sense_column': 'prioridade', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'Descrição', 'sense_column': 'descricao', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'Responsável', 'sense_column': 'responsavel', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'complexidade', 'sense_column': 'complexidade', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'nome projeto', 'sense_column': 'nome_projeto', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'numero sankhya', 'sense_column': 'numero_sankhya', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'status_dt_venc', 'sense_column': 'status_dt_venc', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'Data de criação', 'sense_column': 'data_de_criacao', 'options': 'overwrite', 'type': 'date'}, {'file_column': 'motivo de bloqueio', 'sense_column': 'motivo_de_bloqueio', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'entregue ao cliente', 'sense_column': 'entregue_ao_cliente', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'tipo de solicitação', 'sense_column': 'tipo_de_solicitacao', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'planejamento de entrega', 'sense_column': 'planejamento_de_entrega', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'disponível para faturamento', 'sense_column': 'disponivel_para_faturamento', 'options': 'overwrite', 'type': 'date'}, {'file_column': 'data de aprovação comercial', 'sense_column': 'data_de_aprovacao_comercial', 'options': 'overwrite', 'type': 'date'}], 'sense_customer_key': 'cnpj', 'file_customer_key': 'cnpj', 'custom_data_options': {'ref_date': 'Data de criação', 'type': 'tasks_jira'}, 'truncate': {'type': 'full', 'ref_column': ''}}}, {'label': 'Carregamento [333]', 'task_type': 'load_data', 'description': 'carregamento_333', 'payload': {'output_file': 'jiratarefas/task_333_carregamento.csv', 'input_file': 'jiratarefas/task_332_concatenacao__cnpj_group.csv', 'encoding': 'utf-8-sig', 'delimiter': '|', 'destiny_table': 'custom_data', 'integration_type': 'upsert', 'file_key': ['key'], 'mapping': [{'file_column': 'key', 'sense_column': 'id_task', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'mrr', 'sense_column': 'mrr', 'options': 'overwrite', 'type': 'number'}, {'file_column': 'cnpj', 'sense_column': None, 'options': 'ignore', 'type': 'text'}, {'file_column': 'cnpj2', 'sense_column': None, 'options': 'ignore', 'type': 'text'}, {'file_column': 'e-mail', 'sense_column': 'email', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'fields', 'sense_column': None, 'options': 'ignore', 'type': 'text'}, {'file_column': 'status', 'sense_column': 'status', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'titulo', 'sense_column': 'titulo', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'Prefixo', 'sense_column': 'prefixo', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'contato', 'sense_column': 'contato', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'produto', 'sense_column': 'produto', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'prioridade', 'sense_column': 'prioridade', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'Descrição', 'sense_column': None, 'options': 'ignore', 'type': 'text'}, {'file_column': 'Responsável', 'sense_column': 'responsavel', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'complexidade', 'sense_column': 'complexidade', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'nome projeto', 'sense_column': 'nome_projeto', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'numero sankhya', 'sense_column': 'numero_sankhya', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'status_dt_venc', 'sense_column': 'status_dt_venc', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'Data de criação', 'sense_column': 'data_de_criacao', 'options': 'overwrite', 'type': 'date'}, {'file_column': 'motivo de bloqueio', 'sense_column': 'motivo_de_bloqueio', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'entregue ao cliente', 'sense_column': 'entregue_ao_cliente', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'tipo de solicitação', 'sense_column': 'tipo_de_solicitacao', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'planejamento de entrega', 'sense_column': 'planejamento_de_entrega', 'options': 'overwrite', 'type': 'text'}, {'file_column': 'disponível para faturamento', 'sense_column': 'disponivel_para_faturamento', 'options': 'overwrite', 'type': 'date'}, {'file_column': 'data de aprovação comercial', 'sense_column': 'data_de_aprovacao_comercial', 'options': 'overwrite', 'type': 'date'}], 'sense_customer_key': 'cnpj', 'file_customer_key': 'cnpj2', 'custom_data_options': {'ref_date': 'Data de criação', 'type': 'tasks_jira'}, 'truncate': {'type': 'full', 'ref_column': ''}}}, {'label': 'Separação:cnpj [147]', 'task_type': 'separation', 'description': 'separaocnpj_147', 'payload': {'output_file': 'jiratarefas/task_147_separacaocnpj.csv', 'input_file': 'jiratarefas/task_344_customizada.csv', 'mapping': [{'type': 'json', 'field': 'cnpj', 'attribute': 'customfield_12418', 'field_type': {'type': 'text', 'treatment': 'keep_original', 'remove_zero_left': False, 'remove_whitespaces': True, 'remove_special_char': True, 'remove_dots_and_dash': True}, 'origin_field': 'fields'}]}}, {'label': 'Filtro de Dados:CNPJ_Não_Vazio [148]', 'task_type': 'filter', 'description': 'filtro_de_dadoscnpj_no_vazio_148', 'payload': {'output_file': 'jiratarefas/task_148_filtro_de_dadoscnpj_nao_vazio.csv', 'input_file': 'jiratarefas/task_147_separacaocnpj.csv', 'mapping': [[{'column': 'cnpj', 'condition': 'not_empty', 'equals_to': {'type': 'custom', 'value': ''}}]]}}]}, 'tenant': 'c229'}

name = args['dag_name'].split('.')[0]

dag = DAGBuilder(args['tenant'], name).owner(None).build()

manager = Manager(dag)

# inicial
manager.add_task(LogStart(dag=dag))
manager.add_task(LogEnd(dag=dag))

manager.add_task(WipeStage(dag=dag))


step_fn = {
    "add_or_sub": add_or_sub.add_or_sub,
    "aggregation": aggregation.aggregation,
    "concatenate": concatenation.concatenate,
    "conditional": conditional.conditional,
    "create_column": create_column.create_column,
    "deduplication": deduplication.deduplication,
    "filter": filter.filter,
    "get_data": get_data.get_data,
    "hash_field": hash_field.hash_field,
    "math": math.math_task,
    "merge": merge.merge,
    "load_data": load_data.load_data,
    "separation": separation.separate,
    "truncate": truncate.truncate
}


reload_module = {
    "add_or_sub": add_or_sub,
    "aggregation": aggregation,
    "concatenate": concatenation,
    "conditional": conditional,
    "create_column": create_column,
    "deduplication": deduplication,
    "filter": filter,
    "get_data": get_data,
    "hash_field": hash_field,
    "math": math,
    "merge": merge,
    "load_data": load_data,
    "separation": separation,
    "truncate": truncate
}


custom_tasks = {
    'custom_task': 'tasks.senseconnect.custom_tasks.{tenant}',
    'custom_data_source': 'tasks.senseconnect.custom_data_sources.{tenant}',
    'custom_load_data': 'tasks.senseconnect.custom_load_data.{tenant}',
}


def task_fn(task_type, payload, skip):
    fn = None

    if task_type in step_fn:
        importlib.reload(reload_module[task_type])
        fn = step_fn[task_type]

    elif task_type in custom_tasks:
        source = payload.get("source", "airflow")

        if source == "airflow":
            filename = payload["filename"].split('.')[0]
            classname = payload["classname"]
            module = importlib.import_module(f'.{filename}', package=custom_tasks[task_type].format(tenant=args['tenant']))
            custom_class = getattr(module, classname)
            fn = custom_class().run

        elif source == "kambono":
            fn = step_fn["get_data"]

    if fn is None:
        raise NotImplementedError(f"Task type {task_type} not implemented")

    # # Wait for NTFS to sync the volume
    # if Config.get('environment') == Environment.HOMOLOG:
    #     log.info('Waiting 300 secs')
    #     time.sleep(300)

    try:
        return fn(payload)()
    except Exception:
        if Config.exists('ze_delivery_token'):
            try:
                client, _, workflow_name = name.split('_', 2)
                barman_token = Config.get('ze_delivery_token')
                barman_channel = Config.get('ze_delivery_channel')
                barman = Slack(barman_token)
                barman.send(barman_channel, [
                    {"type": "header", "text": {"type": "plain_text", "text": f":alert: Erro ao executar {workflow_name} - {client} :alert:"}},
                    {"type": "section", "text": {"type": "mrkdwn", "text": f"```{str(traceback.format_exc())}```"}}])
            except Exception:
                pass

        if skip:
            log.error(traceback.format_exc())
            raise AirflowSkipException

        raise

    return


# dinamicos
for key, tasks in args['task_groups'].items():
    with TaskGroup(group_id=key, dag=dag) as group:
        skip_when_fail = not args['groups_dependencies'][key]['args']['skip_when_fail'] # NOQA

        if skip_when_fail:
            trigger_rule = 'none_skipped'
        else:
            trigger_rule = 'none_failed'

        for task in tasks:
            manager.python_task(
                task['description'],
                label=task.get('label', task['description']),
                call=functools.partial(task_fn, task['task_type'], task['payload'], skip_when_fail),
                trigger_rule=trigger_rule,
                op_kwargs={'task_type': task['task_type']}
            )

        manager.add_task(group)

manager.add_deps('wipe_stage', 'log_start')

#task dependencies
for task, dependencies in args['tasks_dependencies'].items():
    for dependency in dependencies:
        manager.add_deps(task, dependency)

#groups dependencies
[
    manager.add_deps(task, depends['dependency'])
    for task, depends in args['groups_dependencies'].items()
]

end_deps = [
    item
    for item in args['groups_dependencies'].keys()
    if item not in args['groups_dependencies'].values()
]


def calc(run_calc='false'):
    if run_calc == 'true':
        CallCalc(dag=dag).execute(get_current_context())
    else:
        raise AirflowSkipException


manager.python_task(calc, keys=['run_calc'], op_kwargs={'task_type': 'calc'})

with manager.add_group('Upload'):
    us = manager.add_task(UploadStage(dag=dag))

    # if Config.get('environment') == Environment.HOMOLOG:
    #     def wait_files():
    #         # Wait for NTFS to sync the volume
    #         log.info('Waiting 300 secs')
    #         time.sleep(300)
    #         return

    #     manager.add_deps(us, manager.python_task(wait_files))

manager.add_deps('calc', *end_deps)
manager.add_deps('Upload', *end_deps)
manager.add_deps('log_end', 'calc', 'Upload')
