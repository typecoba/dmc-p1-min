# 가상환경 생성
python -m venv ./venv

# 가상환경 실행(이하 가상환경 위에서 작동)
cd /venv/Scripts
activate.bat

# requirements 실행
pip install -r requirements.txt

# main.py 실행
python main.py