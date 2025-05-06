import socket
import threading
import re

# Функция для обработки HTTP-запросов от клиента
def handle_client(client_socket):
    try:
        request = client_socket.recv(4096).decode('utf-8', errors='ignore')

        # Первая строка запроса (например: GET / HTTP/1.1)
        request_line = request.split('\r\n')[0]
        match = re.match(r'(\w+)\s+([^\s]+)\s+HTTP/1.1', request_line)

        if not match:
            client_socket.close()
            return

        method, url = match.groups()

        # Извлекаем заголовок Host
        host_match = re.search(r'Host:\s*([^\r\n]+)', request, re.IGNORECASE)
        if not host_match:
            client_socket.close()
            return

        host_header = host_match.group(1)

        # Разделяем хост и порт
        if ':' in host_header:
            host, port = host_header.split(':')
            port = int(port)
        else:
            host = host_header
            port = 80

        # Проверка: абсолютный или относительный URL
        if url.startswith("http://"):
            parsed_url = re.match(r'http://[^/]+(/.*)', url)
            path = parsed_url.group(1) if parsed_url else '/'
        else:
            path = url

        full_url = f"http://{host_header}{path}"

        # Фильтруем ненужные ресурсы, например favicon.ico, .png
        if any(ext in path for ext in ['favicon.ico', 'apple-touch-icon.png', 'apple-touch-icon-precomposed.png']):
            client_socket.close()
            return

        # Подключение к целевому серверу
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.connect((host, port))
            server_socket.send(
                f"{method} {path} HTTP/1.1\r\nHost: {host_header}\r\nConnection: close\r\n\r\n".encode('utf-8')
            )

            response_data = b''
            while True:
                chunk = server_socket.recv(4096)
                if not chunk:
                    break
                response_data += chunk
                try:
                    client_socket.send(chunk)
                except BrokenPipeError:
                    break  # Больше не выводим в консоль

            # Извлечение кода ответа
            try:
                response_line = response_data.decode('utf-8', errors='ignore').split('\r\n')[0]
                status_code = response_line.split(' ')[1]
            except Exception:
                status_code = "???"

            # Логируем результат только для значимых запросов
            if status_code == "200":
                print(f"[Журнал] {method} {full_url} → {status_code}")

    except socket.gaierror:
        print(f"[Ошибка] Не удалось разрешить хост: {host}:{port}")
    except Exception as e:
        print(f"[Ошибка] {e}")
    finally:
        client_socket.close()

# Функция запуска прокси-сервера
def start_proxy_server(host='127.0.0.1', port=8888):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)

    print(f"Прокси-сервер запущен на {host}:{port}...")

    while True:
        client_socket, addr = server.accept()
        print(f"Соединение от {addr}")
        client_thread = threading.Thread(target=handle_client, args=(client_socket,))
        client_thread.start()

if __name__ == "__main__":
    start_proxy_server()
