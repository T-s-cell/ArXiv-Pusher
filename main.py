import os
import io
import re
import json
import zipfile
import requests
from datetime import datetime, timedelta
from arxiv import Client, Search, SortCriterion, SortOrder
from PyPDF2 import PdfReader
import openai

from config import AI_CONFIG, EMAIL_SERVER_CONFIG, GENERAL_CONFIG, USERS_CONFIG, DEFAULT_PROMPT_TEMPLATE, MINERU_CONFIG
from database import get_db

import smtplib
import socket
import asyncio
from email.mime.text import MIMEText
import markdown2  # 导入markdown2库
from loguru import logger
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import time
from bs4 import BeautifulSoup
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed



async def send_email(subject, content, receiver_email):
    """发送邮件通知（异步版本）"""
    # 将Markdown内容转换为HTML；个别总结可能含非法 LaTeX（如双下标）导致
    # latex2mathml 抛异常，此时退化为不渲染公式的转换，保证邮件仍能发出
    try:
        html_content = markdown2.markdown(content, extras=["tables", "latex", "fenced-code-blocks"])
    except Exception as e:
        logger.warning(f"LaTeX 公式渲染失败({type(e).__name__})，退化为无公式渲染: {str(e)[:100]}")
        html_content = markdown2.markdown(content, extras=["tables", "fenced-code-blocks"])
    msg = MIMEText(html_content, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = EMAIL_SERVER_CONFIG["sender"]
    msg["To"] = receiver_email

    server = None
    try:
        logger.info(f"正在连接SMTP服务器，发送给 {receiver_email}...")
        # 将SMTP操作放在线程池中执行，以避免阻塞事件循环
        return await asyncio.get_event_loop().run_in_executor(
            None, lambda: _send_email_sync(msg, server, receiver_email)
        )
    except Exception as e:
        logger.error(f"邮件发送失败: {str(e)}")
        logger.error(f"错误类型: {type(e).__name__}")
        return False


def _send_email_sync(msg, server=None, receiver_email=None):
    """同步发送邮件的内部函数"""
    try:
        server = smtplib.SMTP(
            EMAIL_SERVER_CONFIG["smtp_server"], EMAIL_SERVER_CONFIG["smtp_port"], timeout=10
        )
        server.starttls()  # 启用TLS加密
        server.login(EMAIL_SERVER_CONFIG["sender"], EMAIL_SERVER_CONFIG["password"])

        if receiver_email.count(",") > 0:
            receivers = receiver_email.split(",")
            server.sendmail(EMAIL_SERVER_CONFIG["sender"], receivers, msg.as_string())
        else:
            server.sendmail(
                EMAIL_SERVER_CONFIG["sender"], [receiver_email], msg.as_string()
            )

        logger.success("邮件发送成功")
        return True
    except socket.timeout:
        logger.warning("连接SMTP服务器超时，跳过本次邮件发送")
        return False
    except smtplib.SMTPException as e:
        logger.error(
            f"SMTP错误: {e.smtp_error.decode() if hasattr(e, 'smtp_error') else str(e)}"
        )
        return False
    except Exception as e:
        logger.error(f"邮件发送失败: {str(e)}")
        logger.error(f"错误类型: {type(e).__name__}")
        return False
    finally:
        if server:
            try:
                server.quit()
            except Exception as e:
                logger.warning(f"关闭SMTP连接时发生错误: {str(e)}")


WATERMARK_PATH = "watermark.json"
_last_fetch_max_published = None  # 最近一次 fetch 看到的最大 published 时间（UTC naive）


def _load_watermark():
    """读取上次已处理论文的最大发表时间（UTC naive）；文件不存在或损坏返回 None"""
    try:
        with open(WATERMARK_PATH, encoding="utf-8") as f:
            v = json.load(f).get("last_published_utc")
        return datetime.strptime(v, "%Y-%m-%dT%H:%M:%S") if v else None
    except (FileNotFoundError, ValueError):
        return None


def _save_watermark(dt):
    with open(WATERMARK_PATH, "w", encoding="utf-8") as f:
        json.dump({"last_published_utc": dt.strftime("%Y-%m-%dT%H:%M:%S")}, f)


def fetch_papers(arxiv_categories):
    """获取指定分类的论文
    过滤逻辑用水位线增量：arXiv API 索引比公布晚数小时、且周末批次的 v1 日期跨多天，
    固定取"昨天"会漏论文（2026-10-06 事故），故记录已处理的最大 published 时间，
    每次只取其之后的新论文；无水位线（首次运行）时回退为按回溯天数取上一工作日。"""
    # 构建搜索查询，只包含配置中的主题
    search_query = " OR ".join([f"cat:{cat}" for cat in arxiv_categories])
    client = Client(
        page_size=50,  # 减小每页大小
        delay_seconds=5,  # 请求间隔5秒；2026-10-05 早9点运行曾因HTTP 429中断
        num_retries=8  # 指数退避重试，覆盖约15分钟，扛住arXiv阶段性限流
    )
    search = Search(
        query=search_query,
        sort_by=SortCriterion.SubmittedDate,
        sort_order=SortOrder.Descending,
        max_results=300
    )

    papers = []
    global _last_fetch_max_published
    _last_fetch_max_published = None
    watermark = _load_watermark()
    if watermark is not None:
        logger.info(f"增量模式：只取 published 晚于 {watermark} (UTC) 的论文")
    else:
        # Get the target date (previous workday)
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        target_date = today - timedelta(days=GENERAL_CONFIG["days_lookback"])

        # Adjust if yesterday was a weekend
        weekday = target_date.weekday()  # 0-6, where 5 is Saturday and 6 is Sunday
        if weekday >= 5:  # If Saturday or Sunday
            # Go back to Friday (4)
            target_date -= timedelta(days=weekday - 4)

        logger.info(f"首次运行（无水位线），Target date set to previous workday: {target_date.strftime('%Y-%m-%d')}")
    for result in client.results(search):
        logger.info(f"Processing paper: {result.title} published on {result.published}")
        published_dt = result.published.replace(tzinfo=None)
        if watermark is not None:
            if published_dt <= watermark:
                continue
        elif published_dt < target_date:
            continue
        if _last_fetch_max_published is None or published_dt > _last_fetch_max_published:
            _last_fetch_max_published = published_dt
        papers.append({
            "title": result.title,
            "url": result.entry_id,
            "pdf_url": result.pdf_url,
            "abstract": result.summary,
            "authors": [a.name for a in result.authors],
            "published": result.published,
            "categories": [c for c in result.categories],
            "primary_category": result.primary_category if result.primary_category else None
        })
    since = watermark.strftime('%Y-%m-%d %H:%M') if watermark is not None else target_date.strftime('%Y-%m-%d')
    logger.success(f"Found {len(papers)} papers published after {since} (UTC)")
    return papers

def download_pdf(url, filename, max_retries=3):
    """下载PDF文件，带有重试机制"""
        # 确保URL是正确的PDF链接
    if 'arxiv.org' in url and not url.endswith('.pdf'):
        # 从URL提取论文ID
        paper_id = url.split('/')[-1]
        url = f"https://arxiv.org/pdf/{paper_id}.pdf"
    
    logger.info(f"尝试下载: {url}")
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)  # 添加超时参数
            
            # 检查响应是否成功且内容类型是PDF
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '')
                if 'pdf' not in content_type.lower() and len(response.content) < 10000:
                    logger.warning(f"响应可能不是PDF文件 (Content-Type: {content_type})")
                
                with open(filename, 'wb') as f:
                    f.write(response.content)
                
                # 验证文件大小
                file_size = os.path.getsize(filename)
                if file_size < 1000:  # 小于1KB可能有问题
                    logger.warning(f"下载的文件过小 ({file_size} 字节)")
                    continue
                
                return True
            else:
                logger.error(f"下载失败: HTTP状态码 {response.status_code}")
        except Exception as e:
            logger.warning(f"尝试 {attempt+1}/{max_retries} 失败: {str(e)}")
        
        # 如果不是最后一次尝试，则等待一段时间再重试
        if attempt < max_retries - 1:
            time.sleep(2 * (attempt + 1))  # 指数退避
    
    return False

def extract_text_from_pdf(pdf_path, paper):
    """从PDF提取文本，增加错误处理"""
    text = ""
    try:
        with open(pdf_path, 'rb') as f:
            try:
                reader = PdfReader(f)
                for page_num, page in enumerate(reader.pages):
                    try:
                        page_text = page.extract_text()
                        if page_text:
                            text += page_text + "\n"
                    except Exception as e:
                        logger.warning(f"无法提取第 {page_num+1} 页: {str(e)}")
            except Exception as e:
                logger.error(f"PDF解析失败: {str(e)}")
                # 如果是EOF错误，尝试使用另一种方法
                if "EOF" in str(e):
                    # 可以尝试使用其他库如pdfminer或pdfplumber
                    logger.info("尝试备用PDF解析方法")
                    # 这里可以添加备用解析代码
    except Exception as e:
        logger.error(f"无法打开PDF文件: {str(e)}")
    
    return text

def download_pdf_and_extract_text(paper, user_dir):
    """下载PDF并提取文本，增加错误处理"""
    pdf_path = f"{user_dir}/{paper['title']}.pdf"
    if download_pdf(paper['pdf_url'], pdf_path):
        text = extract_text_from_pdf(pdf_path, paper)
        if not text:
            logger.warning(f"警告: 无法从 {paper['title']} 提取文本")
        return text
    else:
        logger.error(f"错误: 无法下载 {paper['title']} 的PDF")
        return ""

def _mineru_agent_extract(pdf_path, cfg):
    """MinerU Agent 轻量解析（免登录，IP限频；限制≤10MB/约50页，固定轻量模型）"""
    r = requests.post(
        "https://mineru.net/api/v1/agent/parse/file",
        json={
            "file_name": os.path.basename(pdf_path),
            "language": cfg.get("language", "en"),
            "enable_table": True,
            "enable_formula": True,
            "is_ocr": False,
        },
        timeout=30,
    )
    r.raise_for_status()
    d = r.json()
    if d.get("code") != 0:
        logger.warning(f"MinerU agent 申请上传链接失败: {d.get('msg')}")
        return ""
    task_id, upload_url = d["data"]["task_id"], d["data"]["file_url"]

    with open(pdf_path, 'rb') as f:
        put = requests.put(upload_url, data=f, timeout=300)
    if put.status_code != 200:
        logger.warning(f"MinerU agent 文件上传失败: HTTP {put.status_code}")
        return ""

    deadline = time.time() + cfg.get("poll_timeout", 300)
    while time.time() < deadline:
        time.sleep(cfg.get("poll_interval", 3))
        q = requests.get(f"https://mineru.net/api/v1/agent/parse/{task_id}", timeout=30)
        q.raise_for_status()
        st = q.json().get("data") or {}
        state = st.get("state")
        if state == "done":
            md = requests.get(st["markdown_url"], timeout=60)
            md.raise_for_status()
            return md.text
        if state == "failed":
            logger.warning(f"MinerU agent 解析失败: {st.get('err_msg')}")
            return ""
    logger.warning("MinerU agent 轮询超时")
    return ""

def _mineru_precise_extract(pdf_path, cfg):
    """MinerU 精准解析（需 token，≤200MB/200页，每天1000页高优先级额度）"""
    headers = {"Authorization": f"Bearer {cfg.get('token', '')}"}
    safe_name = re.sub(r"[^A-Za-z0-9_.-]", "_", os.path.basename(pdf_path))[:100]
    r = requests.post(
        "https://mineru.net/api/v4/file-urls/batch",
        headers=headers,
        json={
            "files": [{"name": safe_name, "data_id": safe_name}],
            "model_version": cfg.get("model_version", "vlm"),
            "language": cfg.get("language", "en"),
        },
        timeout=30,
    )
    r.raise_for_status()
    d = r.json()
    if d.get("code") != 0:
        logger.warning(f"MinerU precise 申请上传链接失败: {d.get('msg')}")
        return ""
    batch_id, upload_url = d["data"]["batch_id"], d["data"]["file_urls"][0]

    with open(pdf_path, 'rb') as f:
        put = requests.put(upload_url, data=f, timeout=300)
    if put.status_code != 200:
        logger.warning(f"MinerU precise 文件上传失败: HTTP {put.status_code}")
        return ""

    deadline = time.time() + cfg.get("poll_timeout", 300)
    while time.time() < deadline:
        time.sleep(cfg.get("poll_interval", 3))
        q = requests.get(f"https://mineru.net/api/v4/extract-results/batch/{batch_id}", headers=headers, timeout=30)
        q.raise_for_status()
        data = q.json().get("data") or {}
        items = data.get("extract_result") or ([data] if data.get("state") else [])
        item = items[0] if items else {}
        state = item.get("state")
        if state == "done":
            zip_resp = requests.get(item["full_zip_url"], timeout=120)
            zip_resp.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(zip_resp.content)) as zf:
                md_names = [n for n in zf.namelist() if n.endswith("full.md")]
                if not md_names:
                    logger.warning("MinerU precise 结果包中未找到 full.md")
                    return ""
                return zf.read(md_names[0]).decode("utf-8", errors="replace")
        if state == "failed":
            logger.warning(f"MinerU precise 解析失败: {item.get('err_msg')}")
            return ""
    logger.warning("MinerU precise 轮询超时")
    return ""

def mineru_extract_text(pdf_path, paper_title=""):
    """MinerU 云端解析入口：配置了 token 走精准解析 API，否则走免登录 Agent 轻量 API。
    返回 markdown 文本；任何失败返回空串（调用方回退到 PyPDF2 本地提取）。"""
    try:
        cfg = MINERU_CONFIG
        if not cfg.get("enabled", True):
            return ""
        if not os.path.exists(pdf_path) or os.path.getsize(pdf_path) < 1000:
            return ""
        logger.info(f"MinerU 云端解析: {os.path.basename(pdf_path)[:60]}...")
        if cfg.get("token", ""):
            return _mineru_precise_extract(pdf_path, cfg)
        return _mineru_agent_extract(pdf_path, cfg)
    except Exception as e:
        logger.warning(f"MinerU 解析异常，将回退本地提取: {type(e).__name__}: {str(e)[:150]}")
        return ""

def download_html_and_extract_text(paper, user_dir):
    """从arxiv下载HTML版本，保存为PDF，然后提取文本"""
    try:
        # 从paper URL生成HTML链接
        url = paper['url']
        if 'arxiv.org' in url:
            paper_id = url.split('/')[-1]
            html_url = f"https://arxiv.org/html/{paper_id}"
        else:
            html_url = url.replace('.pdf', '.html')

        logger.info(f"尝试下载HTML: {html_url}")

        # 下载HTML内容
        response = requests.get(html_url, timeout=30)

        if response.status_code == 200:
            # 创建一个临时HTML文件
            temp_html_path = f"{user_dir}/{paper['title']}_temp.html"
            with open(temp_html_path, 'wb') as f:
                f.write(response.content)

            # 使用wkhtmltopdf将HTML转换为PDF (需要安装wkhtmltopdf)
            pdf_path = f"{user_dir}/{paper['title']}_from_html.pdf"
            try:
                subprocess.run(['wkhtmltopdf', temp_html_path, pdf_path],
                              check=True, timeout=60)
                logger.info(f"已将HTML转换为PDF: {pdf_path}")
                
                # 尝试从生成的PDF提取文本
                pdf_text = extract_text_from_pdf(pdf_path, paper)
                if pdf_text and len(pdf_text) > 1000:
                    return pdf_text
            except Exception as pdf_err:
                logger.error(f"HTML转PDF失败: {str(pdf_err)}")
            
            # 如果PDF转换失败或提取文本不足，则直接从HTML提取
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 移除脚本和样式元素
            for script in soup(["script", "style"]):
                script.extract()
                
            # 获取文本
            text = soup.get_text(separator="\n", strip=True)
            
            # 处理空白字符
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = '\n'.join(chunk for chunk in chunks if chunk)
            
            logger.info(f"从HTML提取了 {len(text)} 字符的文本")
            return text
        else:
            logger.error(f"HTML下载失败: HTTP状态码 {response.status_code}")
            return ""
    except Exception as e:
        logger.error(f"HTML处理错误: {str(e)}")
        return ""

def get_paper_text(paper, user_dir):
    """尝试多种方式获取论文文本内容
    pdf_extract_mode: "mineru" 优先 MinerU 云端解析（失败回退 PyPDF2）；"pypdf2" 优先本地提取（失败才上云）"""
    pdf_path = f"{user_dir}/{paper['title']}.pdf"
    if not (os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 1000):
        if not download_pdf(paper['pdf_url'], pdf_path):
            logger.error(f"错误: 无法下载 {paper['title']} 的PDF")

    mode = GENERAL_CONFIG.get("pdf_extract_mode", "mineru")
    text = ""
    if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 1000:
        if mode == "pypdf2":
            text = extract_text_from_pdf(pdf_path, paper)
            if not text or len(text) < 1000:
                mineru_text = mineru_extract_text(pdf_path, paper.get('title', ''))
                if len(mineru_text) > len(text or ""):
                    text = mineru_text
        else:
            text = mineru_extract_text(pdf_path, paper.get('title', ''))
            if not text or len(text) < 1000:
                pypdf2_text = extract_text_from_pdf(pdf_path, paper)
                if len(pypdf2_text) > len(text or ""):
                    text = pypdf2_text
        if not text:
            logger.warning(f"警告: 无法从 {paper['title']} 提取文本")

    # 如果PDF方式失败，尝试HTML方式
    if not text or len(text) < 1000:  # 内容太少可能是提取失败
        logger.info(f"PDF提取失败或内容太少，尝试HTML方式")
        text = download_html_and_extract_text(paper, user_dir)

    # 如果text长于129024 则截断
    if len(text) > 129024:
        logger.warning(f"文本内容过长，截断到前129024字符")
        text = text[:129024]
    if not text:
        text = paper['abstract']  # 如果所有方法都失败，使用摘要作为最后的fallback

    return text

def gpt_check_interest(abstract, interest_filter_prompt):
    """使用GPT判断用户是否对论文感兴趣

    Args:
        abstract: 论文摘要
        interest_filter_prompt: 兴趣过滤提示词，需包含{abstract}占位符

    Returns:
        tuple: (bool, dict) 第一个元素表示是否感兴趣，第二个元素为token使用统计
    """
    prompt = interest_filter_prompt.format(abstract=abstract)

    logger.info(f"检查论文兴趣度...")
    # 依次尝试主过滤端点与备用端点（如本地部署模型宕机时回退到云端按量模型）；
    # timeout/max_retries 保证端点挂起时快速失败，避免拖死整个运行
    endpoints = [(
        AI_CONFIG.get("filter_base_url", AI_CONFIG["base_url"]),
        AI_CONFIG.get("filter_api_key", AI_CONFIG["api_key"]),
        AI_CONFIG.get("filter_model", AI_CONFIG["model"]),
    )]
    backup = (
        AI_CONFIG.get("backup_filter_base_url"),
        AI_CONFIG.get("backup_filter_api_key"),
        AI_CONFIG.get("backup_filter_model"),
    )
    if all(backup):
        endpoints.append(backup)

    response = None
    last_err = None
    for base_url, api_key, model in endpoints:
        try:
            client = openai.OpenAI(base_url=base_url, api_key=api_key, timeout=60.0, max_retries=1)
            response = client.chat.completions.create(
                model=model,
                messages=[{
                    "role": "user",
                    "content": prompt
                }],
                temperature=AI_CONFIG.get("filter_temperature", 0.3),  # 降低温度以获得更一致的判断
                extra_body={"thinking": {"type": "enabled" if AI_CONFIG.get("filter_thinking", True) else "disabled"}},  # 思考模式提升判定质量，可经AI_CONFIG.filter_thinking关闭
            )
            break
        except Exception as e:
            last_err = e
            logger.warning(f"过滤端点 {base_url} (模型 {model}) 调用失败: {str(e)}")

    try:
        if response is None:
            raise last_err

        # 记录token使用情况
        usage = response.usage
        token_stats = {
            'prompt_tokens': usage.prompt_tokens,
            'completion_tokens': usage.completion_tokens,
            'total_tokens': usage.total_tokens
        }
        logger.info(f"Token使用 - 输入: {usage.prompt_tokens}, 输出: {usage.completion_tokens}, 总计: {usage.total_tokens}")

        answer = (response.choices[0].message.content or "").strip().upper()
        logger.info(f"兴趣判断结果: {answer[:50]}")

        # 提示词要求模型只输出 Y/N；取回答中出现的首个 Y/N 字符判定
        first = next((ch for ch in answer if ch in ("Y", "N")), None)
        if first == "Y":
            return True, token_stats
        if first == "N":
            return False, token_stats

        # 无法解析时默认保留（保守策略），实际应极少发生
        logger.warning(f"无法解析过滤回答，默认保留。AI回复: {answer[:100]}")
        return True, token_stats

    except Exception as e:
        logger.error(f"兴趣判断失败: {str(e)}，默认为感兴趣")
        return True, {'prompt_tokens': 0, 'completion_tokens': 0, 'total_tokens': 0}  # 出错时默认为感兴趣

def gpt_summarize(text, custom_prompt=None):
    """使用GPT对论文进行总结，支持自定义提示词

    Returns:
        tuple: (str, dict) 第一个元素为总结内容，第二个元素为token使用统计
    """
    # PyPDF2 从部分 PDF 抽取的文本可能含未配对代理字符，会让 HTTP 请求序列化直接失败
    text = text.encode('utf-8', errors='replace').decode('utf-8')

    # 如果没有自定义提示词，使用默认模板
    if custom_prompt:
        prompt = custom_prompt.format(text=text)
    else:
        prompt = DEFAULT_PROMPT_TEMPLATE.format(text=text)

    logger.info(f"Requesting GPT to summarize: {text[:100]}...")
    logger.info(f"Request length: {len(text)}")
    # 依次尝试主总结端点与备用端点（如本地部署模型不可用时回退到云端模型）
    endpoints = [(
        AI_CONFIG.get("summarize_base_url", AI_CONFIG["base_url"]),
        AI_CONFIG.get("summarize_api_key", AI_CONFIG["api_key"]),
        AI_CONFIG.get("summarize_model", AI_CONFIG["model"]),
    )]
    backup = (
        AI_CONFIG.get("backup_summarize_base_url"),
        AI_CONFIG.get("backup_summarize_api_key"),
        AI_CONFIG.get("backup_summarize_model"),
    )
    if all(backup):
        endpoints.append(backup)

    response = None
    last_err = None
    for base_url, api_key, model in endpoints:
        try:
            client = openai.OpenAI(base_url=base_url, api_key=api_key, timeout=1200.0, max_retries=1)
            response = client.chat.completions.create(
                model=model,
                messages=[{
                    "role": "user",
                    "content": prompt
                }],
                temperature=AI_CONFIG.get("temperature", 1.5),
                max_tokens=8192,  # 本地思维链模型思维+正文共用该上限
            )
            break
        except Exception as e:
            last_err = e
            logger.warning(f"总结端点 {base_url} (模型 {model}) 调用失败: {str(e)}")

    if response is None:
        logger.error(f"总结请求失败: {last_err}")
        return f"论文总结生成失败：{last_err}", {'prompt_tokens': 0, 'completion_tokens': 0, 'total_tokens': 0}

    # 记录token使用情况
    usage = response.usage
    token_stats = {
        'prompt_tokens': usage.prompt_tokens,
        'completion_tokens': usage.completion_tokens,
        'total_tokens': usage.total_tokens
    }
    logger.info(f"Token使用 - 输入: {usage.prompt_tokens}, 输出: {usage.completion_tokens}, 总计: {usage.total_tokens}")

    content = response.choices[0].message.content or ""
    logger.info(f"Response: {content[:100]}...")
    logger.info(f"Response length: {len(content)}")
    return content, token_stats

def _log_token_cost(user_name, filter_input_tokens, filter_output_tokens,
                    generate_input_tokens, generate_output_tokens):
    """记录token使用情况和成本

    Args:
        user_name: 用户名称
        filter_input_tokens: 过滤阶段输入token数
        filter_output_tokens: 过滤阶段输出token数
        generate_input_tokens: 生成阶段输入token数
        generate_output_tokens: 生成阶段输出token数
    """
    # 分阶段统计
    filter_total = filter_input_tokens + filter_output_tokens
    generate_total = generate_input_tokens + generate_output_tokens

    # 总计
    total_input_tokens = filter_input_tokens + generate_input_tokens
    total_output_tokens = filter_output_tokens + generate_output_tokens
    total_tokens = total_input_tokens + total_output_tokens

    # 计算成本（元）
    filter_input_cost = (filter_input_tokens / 1_000_000) * AI_CONFIG.get("price_per_million_input_tokens", 0)
    filter_output_cost = (filter_output_tokens / 1_000_000) * AI_CONFIG.get("price_per_million_output_tokens", 0)
    filter_cost = filter_input_cost + filter_output_cost

    generate_input_cost = (generate_input_tokens / 1_000_000) * AI_CONFIG.get("price_per_million_input_tokens", 0)
    generate_output_cost = (generate_output_tokens / 1_000_000) * AI_CONFIG.get("price_per_million_output_tokens", 0)
    generate_cost = generate_input_cost + generate_output_cost

    total_cost = filter_cost + generate_cost

    # 输出统计信息
    logger.info("=" * 80)
    logger.info(f"【{user_name}】Token使用统计:")
    logger.info(f"")
    logger.info(f"过滤阶段:")
    logger.info(f"  输入Token: {filter_input_tokens:,}")
    logger.info(f"  输出Token: {filter_output_tokens:,}")
    logger.info(f"  小计: {filter_total:,} tokens")
    logger.info(f"  成本: ¥{filter_cost:.4f}")
    logger.info(f"")
    logger.info(f"生成阶段:")
    logger.info(f"  输入Token: {generate_input_tokens:,}")
    logger.info(f"  输出Token: {generate_output_tokens:,}")
    logger.info(f"  小计: {generate_total:,} tokens")
    logger.info(f"  成本: ¥{generate_cost:.4f}")
    logger.info(f"")
    logger.info(f"总计:")
    logger.info(f"  输入Token: {total_input_tokens:,}")
    logger.info(f"  输出Token: {total_output_tokens:,}")
    logger.info(f"  总Token数: {total_tokens:,}")
    logger.info(f"  总成本: ¥{total_cost:.4f}")
    logger.info("=" * 80)

def build_filtered_papers_appendix(filtered_out_papers):
    """构建被过滤论文的附录

    Args:
        filtered_out_papers: 被过滤掉的论文列表

    Returns:
        str: 格式化的附录内容
    """
    if not filtered_out_papers:
        return ""

    appendix = ["\n\n" + "=" * 80]
    appendix.append("\n## 📋 附录：其他论文（未通过兴趣过滤）")
    appendix.append("\n以下论文未通过AI兴趣过滤，仅供参考审查：\n")

    for i, paper in enumerate(filtered_out_papers, 1):
        appendix.append(f"\n### {i}. {paper['title']}\n")
        appendix.append(f"**作者**: {', '.join(paper['authors'])}\n")
        appendix.append(f"**发表日期**: {paper['published'].strftime('%Y-%m-%d')}\n")
        appendix.append(f"**链接**: [{paper['url']}]({paper['url']})\n")
        appendix.append(f"**主要分类**: {paper.get('primary_category', '未知分类')}\n")
        appendix.append(f"\n**摘要**:\n{paper['abstract']}\n")
        appendix.append("\n" + "─" * 80 + "\n")

    return ''.join(appendix)

def process_user(user_config):
    """处理单个用户的论文获取和报告生成"""
    user_name = user_config["name"]
    user_email = user_config["email"]
    arxiv_categories = user_config["arxiv_categories"]
    custom_prompt = user_config.get("custom_prompt", None)
    interest_filter_prompt = user_config.get("interest_filter_prompt", None)

    logger.info(f"开始处理用户: {user_name}")

    # 初始化token统计 - 分阶段统计
    filter_input_tokens = 0
    filter_output_tokens = 0
    generate_input_tokens = 0
    generate_output_tokens = 0

    # 初始化论文数量统计
    papers_fetched = 0
    papers_filtered_count = 0
    papers_processed_count = 0

    # 为每个用户创建独立的临时目录
    user_dir = f"temp/{user_name.replace(' ', '_')}"
    os.makedirs(user_dir, exist_ok=True)

    # 获取该用户关注的论文
    papers = fetch_papers(arxiv_categories)
    papers_fetched = len(papers)

    if not papers:
        logger.info(f"用户 {user_name} 没有找到新论文")
        return

    # 第一步：如果配置了兴趣过滤提示词，先根据摘要过滤论文
    filtered_out_papers = []  # 存储被过滤掉的论文
    if interest_filter_prompt:
        logger.info(f"开始使用兴趣过滤（并发模式），共 {len(papers)} 篇论文待过滤")
        filtered_papers = []

        # 定义单个论文过滤任务
        def filter_single_paper(paper_with_index):
            i, paper = paper_with_index
            logger.info(f"过滤论文 {i+1}/{len(papers)}: {paper['title']}")
            try:
                is_interested, token_stats = gpt_check_interest(paper['abstract'], interest_filter_prompt)
                if is_interested:
                    logger.info(f"✓ 用户可能对此论文感兴趣")
                    return ('interested', paper, token_stats)
                else:
                    logger.info(f"✗ 用户可能对此论文不感兴趣，跳过")
                    return ('not_interested', paper, token_stats)
            except Exception as e:
                logger.error(f"过滤论文时出错: {str(e)}，保留该论文")
                return ('error', paper, None)

        # 使用线程池进行并发过滤（降低并发数避免API限流）
        with ThreadPoolExecutor(max_workers=3) as executor:
            # 提交所有任务
            future_to_paper = {executor.submit(filter_single_paper, (i, paper)): paper
                              for i, paper in enumerate(papers)}

            # 收集结果
            for future in as_completed(future_to_paper):
                try:
                    result_type, paper, token_stats = future.result()

                    # 累计token使用
                    if token_stats:
                        filter_input_tokens += token_stats['prompt_tokens']
                        filter_output_tokens += token_stats['completion_tokens']

                    if result_type == 'interested' or result_type == 'error':
                        filtered_papers.append(paper)
                    else:  # not_interested
                        filtered_out_papers.append(paper)

                except Exception as e:
                    logger.error(f"处理过滤结果时出错: {str(e)}")

        papers = filtered_papers
        papers_filtered_count = len(papers)
        logger.info(f"兴趣过滤完成，剩余 {len(papers)} 篇论文，过滤掉 {len(filtered_out_papers)} 篇论文")

        if not papers:
            logger.info(f"用户 {user_name} 经过兴趣过滤后没有感兴趣的论文")
            # 计算成本
            filter_input_cost = (filter_input_tokens / 1_000_000) * AI_CONFIG.get("price_per_million_input_tokens", 0)
            filter_output_cost = (filter_output_tokens / 1_000_000) * AI_CONFIG.get("price_per_million_output_tokens", 0)
            filter_cost = filter_input_cost + filter_output_cost

            # 记录到数据库
            try:
                db = get_db()
                db.record_usage(
                    user_name=user_name,
                    user_email=user_email,
                    arxiv_categories=arxiv_categories,
                    filter_input_tokens=filter_input_tokens,
                    filter_output_tokens=filter_output_tokens,
                    generate_input_tokens=0,
                    generate_output_tokens=0,
                    filter_cost=filter_cost,
                    generate_cost=0.0,
                    papers_fetched=papers_fetched,
                    papers_filtered=0,
                    papers_processed=0
                )
            except Exception as e:
                logger.error(f"记录数据库失败: {str(e)}")

            # 输出成本统计
            _log_token_cost(user_name, filter_input_tokens, filter_output_tokens,
                           generate_input_tokens, generate_output_tokens)
            # 即使没有感兴趣的论文，如果有被过滤的论文，也发送附录
            if filtered_out_papers:
                filtered_appendix = build_filtered_papers_appendix(filtered_out_papers)
                asyncio.run(send_email(f"每日ArXiv论文报告 - {user_name}", filtered_appendix, user_email))
            return
    else:
        # 没有配置兴趣过滤，所有论文都通过
        papers_filtered_count = len(papers)

    # 第二步：根据配置限制处理的论文数量（硬截断）
    max_papers = GENERAL_CONFIG.get("max_papers_per_user", None)
    if max_papers is not None and max_papers > 0:
        papers = papers[:max_papers]
        logger.info(f"应用硬截断，用户 {user_name} 最多处理 {max_papers} 篇论文")

    report = []
    papers_processed_count = 0

    def _summarize_paper(paper):
        """下载全文并生成单篇总结，返回报告片段"""
        # 下载并处理PDF
        text = get_paper_text(paper, user_dir)

        # GPT总结（使用用户自定义提示词）
        summary, token_stats = gpt_summarize(text, custom_prompt)

        # 构建报告
        return token_stats, f"""
## 📄论文标题

{paper['title']}

## 📊 论文信息
* **作者**: {', '.join(paper['authors'])}
* **发表日期**: {paper['published'].strftime('%Y-%m-%d')}
* **链接**: [{paper['url']}]({paper['url']})
* **主要分类**: {paper["primary_category"] if "primary_category" in paper else "未知分类"}
* **所属分类**: {paper["categories"] if "categories" in paper else "未知分类"}
* **摘要原文**:

{paper['abstract']}


## 📝 论文总结
{summary}

{'─' * 80}
"""

    # 并发下载+总结（本地推理模型单篇耗时较长，靠并发压总时长；token统计回主线程累加）
    summarize_workers = GENERAL_CONFIG.get("summarize_workers", 3)
    section_by_idx = {}
    with ThreadPoolExecutor(max_workers=summarize_workers) as executor:
        futures = {executor.submit(_summarize_paper, p): i for i, p in enumerate(papers)}
        for future in as_completed(futures):
            i = futures[future]
            paper = papers[i]
            try:
                token_stats, section = future.result()
                section_by_idx[i] = section
                # 累计生成阶段token使用
                generate_input_tokens += token_stats['prompt_tokens']
                generate_output_tokens += token_stats['completion_tokens']
                papers_processed_count += 1
            except Exception as e:
                logger.error(f"处理论文失败: {paper['title']}，错误: {str(e)}")
                section_by_idx[i] = f"处理论文失败: {paper['title']}，错误: {str(e)}"
    report.extend(section_by_idx[i] for i in sorted(section_by_idx))

    # 输出用户的token使用统计和成本
    _log_token_cost(user_name, filter_input_tokens, filter_output_tokens,
                   generate_input_tokens, generate_output_tokens)

    # 计算成本
    filter_input_cost = (filter_input_tokens / 1_000_000) * AI_CONFIG.get("price_per_million_input_tokens", 0)
    filter_output_cost = (filter_output_tokens / 1_000_000) * AI_CONFIG.get("price_per_million_output_tokens", 0)
    filter_cost = filter_input_cost + filter_output_cost

    generate_input_cost = (generate_input_tokens / 1_000_000) * AI_CONFIG.get("price_per_million_input_tokens", 0)
    generate_output_cost = (generate_output_tokens / 1_000_000) * AI_CONFIG.get("price_per_million_output_tokens", 0)
    generate_cost = generate_input_cost + generate_output_cost

    # 记录到数据库
    try:
        db = get_db()
        db.record_usage(
            user_name=user_name,
            user_email=user_email,
            arxiv_categories=arxiv_categories,
            filter_input_tokens=filter_input_tokens,
            filter_output_tokens=filter_output_tokens,
            generate_input_tokens=generate_input_tokens,
            generate_output_tokens=generate_output_tokens,
            filter_cost=filter_cost,
            generate_cost=generate_cost,
            papers_fetched=papers_fetched,
            papers_filtered=papers_filtered_count,
            papers_processed=papers_processed_count
        )
    except Exception as e:
        logger.error(f"记录数据库失败: {str(e)}")

    if report:
        # 构建完整报告，包括被过滤论文的附录
        full_report = '\n'.join(report)

        # 如果有被过滤掉的论文，添加附录
        if filtered_out_papers:
            full_report += "\n\n" + build_filtered_papers_appendix(filtered_out_papers)

        # 先保存报告到用户专属文件（若后置则发送环节崩溃会丢失全部总结结果）
        report_file = f"{user_dir}/report.md"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(full_report)

        # 发送给该用户
        asyncio.run(send_email(f"每日ArXiv论文报告 - {user_name}", full_report, user_email))
        logger.success(f"用户 {user_name} 的报告已保存到 {report_file}")

        # 发送成功后才推进水位线（严格大于，避免重复处理）；中途崩溃则下次重发同一批
        if _last_fetch_max_published is not None:
            _save_watermark(_last_fetch_max_published)
            logger.info(f"水位线更新至 {_last_fetch_max_published} (UTC)")

def daily_job():
    """每日任务：为所有配置的用户处理论文"""
    os.makedirs('temp', exist_ok=True)

    logger.info(f"开始每日任务，共有 {len(USERS_CONFIG)} 个用户")

    for i, user_config in enumerate(USERS_CONFIG):
        try:
            process_user(user_config)
            # 在处理用户之间添加延迟，避免ArXiv API限流
            if i < len(USERS_CONFIG) - 1:
                logger.info(f"等待60秒后处理下一个用户，避免API限流...")
                time.sleep(60)
        except Exception:
            logger.exception(f"处理用户 {user_config['name']} 时发生错误")

    logger.success("所有用户处理完成")

def run_scheduler():
    scheduler = BlockingScheduler()
    scheduler.add_job(
        daily_job, 
        # arXiv 早上8点(北京)公布的批次，API 索引约滞后6~8小时，14点后才可查，故下午4点运行抓当天批次
        trigger=CronTrigger(hour=16, minute=0),
        id='daily_arxiv_job',
        name='Daily ArXiv paper collection and summary'
    )
    
    logger.info("定时任务已设置，每天下午16:00运行")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("定时任务调度器已停止")

if __name__ == "__main__":
    # 配置loguru
    logger.add(
        "arxiv_pusher.log",
        rotation="10 MB",
        level="INFO",
        encoding="utf-8"
    )
    # 如果需要立即运行一次，取消下面的注释
    # daily_job()
    
    # 启动定时任务
    run_scheduler()