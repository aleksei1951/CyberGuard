# 🤖 CyberGuard

A multifunctional Telegram bot for managing and coordinating militarized communities, gaming clans, or volunteer organizations.

## 🚀 About the Project

"CyberGuard" provides a hierarchical role system, task (mission) management, and an internal support system (tickets).

### ⚡ Development Features

The project is an example of modern rapid development:
- Created from scratch in 3 days
- Developed using neural networks (LLM)
- Demonstrates human-AI synergy in development

## 💫 Main Features

### 👤 For All Soldiers

- ✅ **Ready Status**: Confirm readiness for tasks
- 📊 **Personal Status**: View statistics, callsign, and active missions
- 📨 **Command Report**: Create support tickets
- 📝 **Mission Execution**: Receive and mark orders as completed

### 👑 For Command Staff

- ⚡ **Create Orders**: Flexible mission creation system
- 🔐 **Mission Approval**: Centurions can approve/reject missions
- 📈 **Combat Summary**: Overall statistics on missions and personnel
- ⚙️ **Unit Management**: Manage soldier lists
- 🎫 **Ticket Processing**: Handle support tickets
- 📞 **Callsign Setup**: Manage soldier callsigns

## 🏰 Role Hierarchy

| Role | Description |
|------|-------------|
| 👑 **Administrator** | Main commander with full access |
| 🎖️ **Centurion** | Senior command staff |
| ⭐ **Decurion** | Junior command staff |
| 👤 **Private** | Main personnel |

## 🚀 Installation and Launch

### 1️⃣ Clone Repository
```bash
git clone https://github.com/aleksei1951/CyberGuard.git
cd CyberGuard
```

### 2️⃣ Install Dependencies
```bash
pip install aiogram
```

### 3️⃣ Configuration
Open `main.py` and configure the Config class:

```python
class Config:
    # Your bot token
    TOKEN = "YOUR_TOKEN"
    
    # Admin Telegram IDs
    ADMIN_IDS = {YOUR_ID}
```

- **TOKEN**: Get from [@BotFather](https://t.me/BotFather)
- **ADMIN_IDS**: Enter your Telegram ID

### 4️⃣ Launch
```bash
python main.py
```

## 🛠️ Project Structure

The project is organized in a single file `7.py` and includes:

- 📝 **Configuration** (Config)
- 🏗️ **Data Models** (UnitType, MissionStatus)
- 🎮 **Command Handlers**
- 📊 **Data Management**
- 🔄 **Utilities and Helper Functions**

## 📌 Additional Information

After launch, the bot is ready to work. Administrators specified in ADMIN_IDS automatically receive Centurion rights.

## 🔑 Key Features

- Clear role hierarchy system
- Mission management with approval workflow
- Internal ticket system
- Statistical tracking and reporting
- User-friendly interface with inline buttons
- Flexible configuration options
