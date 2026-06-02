from airflow.utils.email import send_email


def dag_success_callback(context):

    dag_run = context["dag_run"]
    dag_id = dag_run.dag_id

    subject = f"[SUCCESS] DAG {dag_id}"

    html_content = f"""
    <h3>✅ DAG Success</h3>

    <table border="1" cellpadding="5">
        <tr>
            <td><b>DAG ID</b></td>
            <td>{dag_id}</td>
        </tr>

        <tr>
            <td><b>Run ID</b></td>
            <td>{dag_run.run_id}</td>
        </tr>

        <tr>
            <td><b>Execution Date</b></td>
            <td>{context.get('logical_date')}</td>
        </tr>
    </table>
    """

    send_email(
        to=["siharpangaribuan03@gmail.com"],
        subject=subject,
        html_content=html_content
    )


def dag_failure_callback(context):

    ti = context["task_instance"]
    dag_run = context["dag_run"]

    subject = f"[FAILED] DAG {ti.dag_id}"

    html_content = f"""
    <h3>❌ DAG Failed</h3>

    <table border="1" cellpadding="5">
        <tr>
            <td><b>DAG ID</b></td>
            <td>{ti.dag_id}</td>
        </tr>

        <tr>
            <td><b>Task ID</b></td>
            <td>{ti.task_id}</td>
        </tr>

        <tr>
            <td><b>Run ID</b></td>
            <td>{dag_run.run_id}</td>
        </tr>

        <tr>
            <td><b>Execution Date</b></td>
            <td>{context.get('logical_date')}</td>
        </tr>

        <tr>
            <td><b>Log URL</b></td>
            <td>
                <a href="{ti.log_url}">
                    Open Log
                </a>
            </td>
        </tr>

        <tr>
            <td><b>Error</b></td>
            <td>{context.get('exception')}</td>
        </tr>
    </table>
    """

    send_email(
        to=["siharpangaribuan03@gmail.com"],
        subject=subject,
        html_content=html_content
    )
