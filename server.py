# server.py
import asyncio
import json
import aiohttp
from aiohttp import web
from pynput import keyboard, mouse
import utils
from templates import HTML_PAGE

# 全局状态
connected_clients = set()
authenticated_client = None  # 当前已配对的唯一客户端
client_configs = {}
synced_text = ""
rebase_triggered = False  # 标记是否已触发增量模式
pending_strip_punctuation = False  # 标记重置后的第一笔输入是否剔除标点
main_loop = None
_runner = None

def is_client_connected():
    return authenticated_client is not None and not authenticated_client.closed

def is_active_client(ws):
    """校验该连接是否仍是当前授权客户端（踢人/断开后立即失效）"""
    return ws is authenticated_client and not ws.closed

def reset_sync_state():
    """重置同步相关状态"""
    global synced_text, rebase_triggered, pending_strip_punctuation
    synced_text = ""
    rebase_triggered = False
    pending_strip_punctuation = False

def _clear_client(ws):
    """从服务端移除指定客户端引用"""
    global authenticated_client
    connected_clients.discard(ws)
    client_configs.pop(ws, None)
    if ws is authenticated_client:
        authenticated_client = None
        reset_sync_state()

async def _async_close_ws(ws, kicked=False, reason=None):
    if ws.closed:
        return
    try:
        if kicked:
            payload = {'type': 'kicked'}
            if reason:
                payload['reason'] = reason
            await ws.send_json(payload)
        await ws.close()
    except Exception:
        pass

async def _async_disconnect_client(kicked=False):
    """在事件循环内断开当前授权客户端"""
    if not is_client_connected():
        return False
    ws = authenticated_client
    _clear_client(ws)
    await _async_close_ws(ws, kicked=kicked)
    return True

async def _async_shutdown(notify=False):
    """优雅关闭所有连接并清理 runner"""
    global authenticated_client, _runner
    clients = list(connected_clients)
    authenticated_client = None
    connected_clients.clear()
    client_configs.clear()
    reset_sync_state()
    for ws in clients:
        await _async_close_ws(ws, kicked=notify, reason='server_restart' if notify else None)
    if _runner is not None:
        await _runner.cleanup()
        _runner = None

def _run_on_loop(coro, timeout=5):
    if not main_loop or not main_loop.is_running():
        return None
    future = asyncio.run_coroutine_threadsafe(coro, main_loop)
    return future.result(timeout=timeout)

def disconnect_client(kicked=False):
    """从桌面端主动断开已连接的手机"""
    try:
        return bool(_run_on_loop(_async_disconnect_client(kicked=kicked), timeout=3))
    except Exception as e:
        print(f"断开连接失败：{e}")
        return False

def shutdown_server(notify=False, stop_loop=True):
    """优雅停止服务：关闭连接、清理 runner，可选停止事件循环"""
    global main_loop, authenticated_client
    loop = main_loop
    if not loop or not loop.is_running():
        reset_sync_state()
        authenticated_client = None
        connected_clients.clear()
        client_configs.clear()
        return
    try:
        _run_on_loop(_async_shutdown(notify=notify), timeout=5)
    except Exception as e:
        print(f"关闭服务时出错：{e}")
    if stop_loop:
        loop.call_soon_threadsafe(loop.stop)

def broadcast_config():
    """广播配置更新到所有连接的客户端"""
    global main_loop
    if not main_loop:
        return
    
    config = {
        'type': 'config',
        'backspaceLimit': utils.get_backspace_limit(),
        'autoClear': utils.get_auto_clear(),
        'autoClearTime': utils.get_auto_clear_time(),
        'smartDetection': utils.get_smart_detection()
    }
    
    for ws in list(connected_clients):
        if not ws.closed:
            asyncio.run_coroutine_threadsafe(ws.send_json(config), main_loop)

async def handle_ws(req):
    global synced_text, rebase_triggered, pending_strip_punctuation, authenticated_client
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    ui_callback = req.app.get('ui_callback')

    if is_client_connected():
        await ws.send_json({'type': 'connect', 'ok': False, 'reason': 'occupied'})
        await ws.close()
        return ws

    authenticated_client = ws
    connected_clients.add(ws)
    await ws.send_json({'type': 'connect', 'ok': True})
    if ui_callback:
        ui_callback(True)

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                if not is_active_client(ws):
                    continue
                data = json.loads(msg.data)
                
                # 处理配置（如键盘检测开关）
                if data.get('type') == 'config':
                    client_configs[ws] = {'detect_keyboard': data.get('detectKeyboard')}
                
                # 核心同步逻辑
                elif data.get('type') == 'diff':
                    new_txt = data.get('newText', '')
                    d_cnt, add_txt = utils.compute_diff(synced_text, new_txt)
                    
                    # 【安全锁定】如果刚才电脑介入过，本次输入强制不退格
                    if rebase_triggered:
                        d_cnt = 0
                        rebase_triggered = False
                    
                    # 处理语音输入法自动带出的首位标点
                    if pending_strip_punctuation and d_cnt == 0 and add_txt:
                        punc = "，。、；：？！\"\"''·…—～,.;:?!'\""
                        if add_txt[0] in punc: 
                            add_txt = add_txt[1:]
                        pending_strip_punctuation = False
                    
                    # 执行电脑端操作
                    if d_cnt: 
                        utils.send_backspaces(d_cnt)
                    if add_txt: 
                        utils.type_text(add_txt)
                    
                    synced_text = new_txt
                
                # 处理手动重置
                elif data.get('type') == 'reset':
                    synced_text = ""
                    pending_strip_punctuation = True
                    rebase_triggered = False

    finally:
        was_active = ws is authenticated_client
        _clear_client(ws)
        if was_active and ui_callback:
            ui_callback(False)
    return ws

def start_server_thread(loop, ui_callback):
    global main_loop, authenticated_client, _runner
    main_loop = loop
    asyncio.set_event_loop(loop)
    authenticated_client = None
    connected_clients.clear()
    client_configs.clear()
    reset_sync_state()
    
    app = web.Application()
    app['ui_callback'] = ui_callback
    # 生成带有自动清空设置的 HTML 页面
    def get_html_page():
        auto_clear_enabled = utils.get_auto_clear()
        auto_clear_time = utils.get_auto_clear_time()
        html = HTML_PAGE.replace('id="auto_clr"', f'id="auto_clr" {"checked" if auto_clear_enabled else ""}')
        html = html.replace('15000', str(auto_clear_time * 1000))
        return html
    app.router.add_get('/', lambda r: web.Response(text=get_html_page(), content_type='text/html'))
    app.router.add_get('/ws', handle_ws)
    
    _runner = web.AppRunner(app)
    loop.run_until_complete(_runner.setup())
    loop.run_until_complete(web.TCPSite(_runner, '0.0.0.0', utils.get_port()).start())
    
    # --- 智能感知：监听电脑动作 ---
    def reset_synced_text():
        global synced_text, rebase_triggered, pending_strip_punctuation
        if utils.is_typing() or not connected_clients: return
        
        # 检查全局智能感知开关状态
        if not utils.get_smart_detection():
            return
        
        # 只要动了电脑，就重置后端状态并通知手机锁定锚点
        if synced_text and any(c.get('detect_keyboard') for c in client_configs.values()):
            synced_text = ""
            rebase_triggered = True
            pending_strip_punctuation = True
            for ws in list(connected_clients):
                if not ws.closed:
                    asyncio.run_coroutine_threadsafe(ws.send_json({'type': 'rebase'}), loop)

    # 监听键盘（仅限字符输入）和鼠标左键
    keyboard.Listener(on_press=lambda k: reset_synced_text() if hasattr(k, 'char') else None).start()
    mouse.Listener(on_click=lambda x,y,b,p: reset_synced_text() if p and b==mouse.Button.left else None).start()
    
    loop.run_forever()