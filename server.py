import asyncio
import json
import random
import time
from datetime import datetime
from websockets import serve, WebSocketServerProtocol

# Хранилище комнат
rooms = {}
# { room_id: { 
#     'players': { player_id: { 'name': str, 'ws': WebSocket, 'score': int, 'current': str, 'prev': str, 'operator': str } },
#     'game_state': 'waiting' | 'racing' | 'finished',
#     'timer': int,
#     'messages': []
# }}

# Хранилище подключений
connections = {}

def generate_room_id():
    return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))

async def broadcast_room(room_id, message):
    """Отправить сообщение всем в комнате"""
    if room_id not in rooms:
        return
    room = rooms[room_id]
    for player_id, player in room['players'].items():
        try:
            await player['ws'].send(json.dumps(message))
        except:
            pass

async def handle_connection(websocket, path):
    """Обработка подключения"""
    try:
        # Ждём первое сообщение (регистрация)
        data = await websocket.recv()
        msg = json.loads(data)
        
        if msg['type'] != 'join':
            await websocket.close()
            return
        
        player_name = msg.get('name', 'Аноним')
        room_id = msg.get('room_id')
        
        # Если комната не указана или не существует, создаём новую
        if not room_id or room_id not in rooms:
            room_id = generate_room_id()
            rooms[room_id] = {
                'players': {},
                'game_state': 'waiting',
                'timer': 60,
                'messages': [],
                'start_time': None,
                'winner': None
            }
        
        room = rooms[room_id]
        player_id = str(time.time()) + str(random.randint(1000, 9999))
        
        # Добавляем игрока
        room['players'][player_id] = {
            'name': player_name,
            'ws': websocket,
            'score': 0,
            'current': '0',
            'prev': '',
            'operator': None,
            'reset_after_equals': False
        }
        
        connections[player_id] = websocket
        
        # Отправляем ID игрока
        await websocket.send(json.dumps({
            'type': 'init',
            'player_id': player_id,
            'room_id': room_id,
            'game_state': room['game_state'],
            'timer': room['timer'],
            'players': {pid: {'name': p['name'], 'score': p['score']} 
                       for pid, p in room['players'].items()},
            'messages': room['messages'][-20:]
        }))
        
        # Оповещаем всех о новом игроке
        await broadcast_room(room_id, {
            'type': 'player_joined',
            'player_id': player_id,
            'name': player_name,
            'players': {pid: {'name': p['name'], 'score': p['score']} 
                       for pid, p in room['players'].items()}
        })
        
        # Основной цикл обработки сообщений
        async for message in websocket:
            try:
                data = json.loads(message)
                await process_message(room_id, player_id, data)
            except json.JSONDecodeError:
                continue
                
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        # Удаляем игрока
        player_id_to_remove = None
        room_id_to_remove = None
        for rid, room in rooms.items():
            for pid in list(room['players'].keys()):
                if room['players'][pid]['ws'] == websocket:
                    player_id_to_remove = pid
                    room_id_to_remove = rid
                    break
            if player_id_to_remove:
                break
        
        if player_id_to_remove and room_id_to_remove:
            room = rooms[room_id_to_remove]
            del room['players'][player_id_to_remove]
            
            # Если комната пуста, удаляем её
            if not room['players']:
                del rooms[room_id_to_remove]
            else:
                await broadcast_room(room_id_to_remove, {
                    'type': 'player_left',
                    'player_id': player_id_to_remove,
                    'players': {pid: {'name': p['name'], 'score': p['score']} 
                               for pid, p in room['players'].items()}
                })

async def process_message(room_id, player_id, data):
    """Обработка сообщений от игрока"""
    if room_id not in rooms:
        return
    
    room = rooms[room_id]
    player = room['players'].get(player_id)
    if not player:
        return
    
    msg_type = data.get('type')
    
    if msg_type == 'chat':
        # Сообщение в чат
        text = data.get('text', '')
        room['messages'].append({
            'player_id': player_id,
            'name': player['name'],
            'text': text,
            'time': time.time()
        })
        await broadcast_room(room_id, {
            'type': 'chat',
            'player_id': player_id,
            'name': player['name'],
            'text': text
        })
    
    elif msg_type == 'calculator':
        # Действие на калькуляторе
        action = data.get('action')
        value = data.get('value')
        
        if room['game_state'] == 'racing':
            # Обработка вычислений
            result = process_calculation(player, action, value)
            if result and result.get('score_change'):
                player['score'] += result['score_change']
                player['score'] = round(player['score'], 2)
            
            await broadcast_room(room_id, {
                'type': 'calculator_update',
                'player_id': player_id,
                'display': player['current'],
                'score': player['score'],
                'result': result
            })
    
    elif msg_type == 'start_race':
        # Запуск гонки (только создатель комнаты)
        if len(room['players']) >= 2 and room['game_state'] == 'waiting':
            room['game_state'] = 'racing'
            room['timer'] = 60
            room['start_time'] = time.time()
            
            # Сбрасываем счёт
            for p in room['players'].values():
                p['score'] = 0
                p['current'] = '0'
                p['prev'] = ''
                p['operator'] = None
            
            await broadcast_room(room_id, {
                'type': 'race_started',
                'timer': 60
            })
            
            # Запускаем таймер
            asyncio.create_task(race_timer(room_id))
    
    elif msg_type == 'reset_scores':
        # Сброс счёта
        for p in room['players'].values():
            p['score'] = 0
            p['current'] = '0'
            p['prev'] = ''
            p['operator'] = None
        
        await broadcast_room(room_id, {
            'type': 'scores_reset',
            'players': {pid: {'name': p['name'], 'score': p['score']} 
                       for pid, p in room['players'].items()}
        })
    
    elif msg_type == 'reset_game':
        # Полный сброс игры
        room['game_state'] = 'waiting'
        room['timer'] = 60
        room['winner'] = None
        for p in room['players'].values():
            p['score'] = 0
            p['current'] = '0'
            p['prev'] = ''
            p['operator'] = None
        
        await broadcast_room(room_id, {
            'type': 'game_reset',
            'game_state': 'waiting',
            'players': {pid: {'name': p['name'], 'score': p['score']} 
                       for pid, p in room['players'].items()}
        })

def process_calculation(player, action, value):
    """Обработка логики калькулятора"""
    result = {'action': action, 'value': value, 'score_change': 0}
    
    if player['reset_after_equals'] and action != 'equals' and action != 'clear':
        if action == 'digit':
            player['current'] = '0.' if value == '.' else value
            player['reset_after_equals'] = False
            return result
        if action == 'operator':
            player['prev'] = player['current']
            player['operator'] = value
            player['reset_after_equals'] = False
            return result
    
    if action == 'digit':
        if len(player['current']) >= 16:
            return result
        if player['current'] == '0' and value != '.':
            player['current'] = value
        elif value == '.' and '.' in player['current']:
            return result
        else:
            player['current'] += value
        player['reset_after_equals'] = False
        
    elif action == 'operator':
        if player['operator'] and player['prev'] and player['current']:
            calc_result = compute(player['prev'], player['operator'], player['current'])
            if calc_result is not None:
                player['current'] = calc_result
                player['prev'] = calc_result
                player['operator'] = value
                result['display'] = player['current']
        else:
            if player['current'] and player['current'] != '-':
                player['prev'] = player['current']
                player['operator'] = value
        player['reset_after_equals'] = False
        
    elif action == 'equals':
        if player['operator'] and player['prev']:
            calc_result = compute(player['prev'], player['operator'], player['current'])
            if calc_result is not None:
                num_result = float(calc_result)
                score_change = abs(num_result)
                result['score_change'] = score_change
                player['current'] = calc_result
                player['prev'] = ''
                player['operator'] = None
                player['reset_after_equals'] = True
                
    elif action == 'clear':
        player['current'] = '0'
        player['prev'] = ''
        player['operator'] = None
        player['reset_after_equals'] = False
        
    elif action == 'backspace':
        if len(player['current']) > 1:
            player['current'] = player['current'][:-1]
        else:
            player['current'] = '0'
        player['reset_after_equals'] = False
    
    return result

def compute(prev, operator, curr):
    """Выполнение вычисления"""
    try:
        a = float(prev)
        b = float(curr)
        if operator == '+': result = a + b
        elif operator == '-': result = a - b
        elif operator == '*': result = a * b
        elif operator == '/':
            if b == 0: return None
            result = a / b
        elif operator == '%':
            if b == 0: return None
            result = a % b
        else: return None
        
        if not float.is_integer(result):
            result = round(result, 10)
        return str(result)
    except:
        return None

async def race_timer(room_id):
    """Таймер гонки"""
    if room_id not in rooms:
        return
    
    room = rooms[room_id]
    while room['timer'] > 0 and room['game_state'] == 'racing':
        await asyncio.sleep(1)
        room['timer'] -= 1
        
        await broadcast_room(room_id, {
            'type': 'timer_update',
            'timer': room['timer']
        })
    
    if room['game_state'] == 'racing':
        room['game_state'] = 'finished'
        # Определяем победителя
        winner = None
        max_score = -1
        for pid, p in room['players'].items():
            if p['score'] > max_score:
                max_score = p['score']
                winner = pid
        
        await broadcast_room(room_id, {
            'type': 'race_finished',
            'winner': winner,
            'winner_name': room['players'][winner]['name'] if winner else None,
            'players': {pid: {'name': p['name'], 'score': p['score']} 
                       for pid, p in room['players'].items()}
        })

async def main():
    print("🚀 Сервер запущен на ws://localhost:8765")
    async with serve(handle_connection, "0.0.0.0", 8765):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
