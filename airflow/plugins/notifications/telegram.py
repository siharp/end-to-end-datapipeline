import logging
import requests
from airflow.models import Variable

log = logging.getLogger(__name__)


def send_telegram_message(bot_token: str, chat_id: str, message: str):
    """Kirim pesan ke Telegram."""
    try:
        response = requests.post(
            url=f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            },
            timeout=10
        )
        response.raise_for_status()
        log.info("Notifikasi Telegram berhasil dikirim")

    except requests.exceptions.Timeout:
        log.error("Timeout saat kirim notifikasi Telegram")
    except requests.exceptions.HTTPError as e:
        log.error(f"HTTP error saat kirim notifikasi Telegram: {e}")
    except Exception as e:
        log.error(f"Gagal kirim notifikasi Telegram: {e}")


def get_telegram_config():
    """Ambil config Telegram dari Airflow Variable."""
    try:
        tg_conf = Variable.get("telegram_config", deserialize_json=True)
        bot_token = tg_conf.get('bot_token')
        chat_id = tg_conf.get('chat_id')

        if not bot_token or not chat_id:
            log.error("telegram_config tidak lengkap")
            return None, None

        return bot_token, chat_id

    except Exception as e:
        log.error(f"Gagal ambil telegram_config: {e}")
        return None, None


def notify_failure(context):
    """Callback saat task gagal."""
    try:
        ti = context['task_instance']
        task_id = ti.task_id
        dag_id = ti.dag_id
        run_id = context['run_id']
        log_url = ti.log_url
        execution_date = context['execution_date']
        exception = context.get('exception', 'No exception info')

        bot_token, chat_id = get_telegram_config()
        if not bot_token:
            return

        message = (
            f"❌ *Airflow Task Failed*\n\n"
            f"📌 *DAG:* `{dag_id}`\n"
            f"📌 *Task:* `{task_id}`\n"
            f"🕐 *Execution Date:* `{execution_date}`\n"
            f"🆔 *Run ID:* `{run_id}`\n"
            f"⚠️ *Error:*\n`{str(exception)[:500]}`\n\n"
            f"📋 [Lihat Log]({log_url})"
        )

        send_telegram_message(bot_token, chat_id, message)

    except Exception as e:
        log.error(f"notify_failure error: {e}")


def notify_success(context):
    """Callback saat DAG sukses."""
    try:
        ti = context['task_instance']
        dag_id = ti.dag_id
        run_id = context['run_id']
        execution_date = context['execution_date']

        bot_token, chat_id = get_telegram_config()
        if not bot_token:
            return

        message = (
            f"✅ *Airflow DAG Success*\n\n"
            f"📌 *DAG:* `{dag_id}`\n"
            f"🕐 *Execution Date:* `{execution_date}`\n"
            f"🆔 *Run ID:* `{run_id}`\n"
        )

        send_telegram_message(bot_token, chat_id, message)

    except Exception as e:
        log.error(f"notify_success error: {e}")
