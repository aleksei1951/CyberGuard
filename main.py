# --- IMPORTS ---
import json
import logging
import re
import asyncio
from datetime import datetime, timedelta
from collections import deque, defaultdict
from typing import Dict, List, Set, Deque, Optional, Any, Tuple, cast
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramForbiddenError

# --- SYSTEM CONFIGURATION ---
class Config:
    TOKEN = ""  # Get from @botfather
    DATA_FILE = "cyber_squad_prod.json"
    BACKUP_FILE = "cyber_squad_backup.json"
    LOG_FILE = "cyber_guardian.log"
    MAX_LAST_MISSIONS = 15
    MAX_CALL_SIGN_LENGTH = 20
    ADMIN_IDS = {}  # Creator role. Uses profile ID.
    AUTO_SAVE_INTERVAL = 300  # In seconds
    TICKET_TIMEOUT = 72  # Hours until inactive ticket closure
    MAX_MESSAGE_LENGTH = 4096  # Maximum Telegram message length
    MAX_MISSION_NAME_LENGTH = 50  # Maximum mission name length

# --- LOGGING SETUP ---
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(Config.LOG_FILE, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("CyberGuard")
logger = setup_logging()

# --- BOT INITIALIZATION ---
bot = Bot(
    token=Config.TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
dp = Dispatcher()
main_router = Router()
dp.include_router(main_router)

# --- DATA MODELS ---
class UnitType:
    CENTURIONS = "centurions"
    DECURIONS = "decurions"
    PRIVATES = "privates"
    ALL_TYPES = [CENTURIONS, DECURIONS, PRIVATES]

class MissionStatus:
    PENDING = "Pending"
    ACTIVE = "Active"
    COMPLETED = "Completed"
    APPROVED = "Approved"
    REJECTED = "Rejected"

class ButtonText:
    COMBAT_READY = "Ready for Action!"
    REPORT = "Report to Command"
    MY_STATUS = "My Status"
    HELP = "/help"
    CREATE_MISSION = "Create Mission"
    STATS = "Operation Summary"
    MANAGE_UNITS = "Manage Units"
    SET_CALL_SIGN = "Set Callsign"
    ACTIVE_TICKETS = "Active Tickets"
    CLOSE_TICKET = "Close Ticket"

# --- SYSTEM CORE ---
class DataManager:
    def __init__(self):
        self.data = {
            "units": {unit_type: set() for unit_type in UnitType.ALL_TYPES},
            "missions": {
                "active": deque(maxlen=Config.MAX_LAST_MISSIONS),
                "archive": {},
                "approvals": {}
            },
            "command": {
                "call_signs": {},
                "tickets": {},
                "activity": {},
                "temp_actions": {},
                "temp_missions": {},
                "user_active_tickets": {},
                "ticket_responses": {}  # Structure: {ticket_id: {commander_id: {chat_id, message_id}}}
            },
            "subscribers": set(),
            "combat_ready": set(),
            "usernames": {}  # Username cache
        }
        self._load_initial_data()
    
    def _load_initial_data(self):
        try:
            with open(Config.DATA_FILE, "r", encoding='utf-8') as f:
                raw_data = json.load(f)
                self._convert_data(raw_data)
            logger.info("Operational data loaded")
        except Exception as e:
            logger.warning(f"Initializing new data. Reason: {e}")
            self._add_default_commanders()
    
    def _convert_data(self, raw_data: dict):
        """Convert loaded data into working structures"""
        self.data["units"] = {
            unit_type: set(raw_data["units"].get(unit_type, []))
            for unit_type in UnitType.ALL_TYPES
        }
        
        # Конвертация missions
        self.data["missions"]["active"] = deque(
            raw_data["missions"].get("active", []), 
            maxlen=Config.MAX_LAST_MISSIONS
        )
        
        archive = raw_data["missions"].get("archive", {})
        for mission_id, mission in archive.items():
            if "completed_by" in mission:
                mission["completed_by"] = set(mission["completed_by"])
        self.data["missions"]["archive"] = archive
        
        # Конвертация command
        self.data["command"]["call_signs"] = raw_data["command"].get("call_signs", {})
        activity_data = raw_data["command"].get("activity", {})
        self.data["command"]["activity"] = {
            int(k): datetime.fromisoformat(v) 
            for k, v in activity_data.items()
        }
        self.data["command"]["temp_actions"] = raw_data["command"].get("temp_actions", {})
        self.data["command"]["temp_missions"] = raw_data["command"].get("temp_missions", {})
        self.data["command"]["user_active_tickets"] = raw_data["command"].get("user_active_tickets", {})
        
        # Конвертация ticket_responses с новой структурой
        raw_responses = raw_data["command"].get("ticket_responses", {})
        self.data["command"]["ticket_responses"] = {
            ticket_id: {
                int(cmdr_id): info 
                for cmdr_id, info in responses.items()
            } for ticket_id, responses in raw_responses.items()
        }
        
        # Конвертация множеств
        self.data["subscribers"] = set(raw_data.get("subscribers", []))
        self.data["combat_ready"] = set(raw_data.get("combat_ready", []))
        
        # Конвертация usernames
        self.data["usernames"] = raw_data.get("usernames", {})
    
    def _add_default_commanders(self):
        """Add default command structure"""
        for admin_id in Config.ADMIN_IDS:
            self.add_commander(admin_id, UnitType.CENTURIONS)
    
    def add_commander(self, user_id: int, unit_type: str):
        """Add a commander to the system"""
        if unit_type not in UnitType.ALL_TYPES:
            raise ValueError(f"Unknown unit type: {unit_type}")
        self.data["units"][unit_type].add(user_id)
        self.data["command"]["activity"][user_id] = datetime.now()
        self.data["subscribers"].add(user_id)
        logger.info(f"New {unit_type} commander: {user_id}")
    
    def save_data(self):
        """Save data with error handling"""
        try:
            data_to_save = self._prepare_data_for_saving()
            with open(Config.DATA_FILE, "w", encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=2)
            self._create_backup(data_to_save)
            logger.info("Data saved successfully")
            return True
        except Exception as e:
            logger.error(f"Save error: {e}")
            return False
    
    def _prepare_data_for_saving(self) -> dict:
        """Prepare data for serialization"""
        return {
            "units": {k: list(v) for k, v in self.data["units"].items()},
            "missions": {
                "active": list(self.data["missions"]["active"]),
                "archive": {
                    mid: {
                        **mission,
                        "completed_by": list(mission.get("completed_by", []))
                    } for mid, mission in self.data["missions"]["archive"].items()
                },
                "approvals": self.data["missions"]["approvals"]
            },
            "command": {
                "call_signs": self.data["command"]["call_signs"],
                "tickets": self.data["command"]["tickets"],
                "activity": {
                    k: v.isoformat() for k, v in self.data["command"]["activity"].items()
                },
                "temp_actions": self.data["command"]["temp_actions"],
                "temp_missions": self.data["command"]["temp_missions"],
                "user_active_tickets": self.data["command"]["user_active_tickets"],
                "ticket_responses": {
                    ticket_id: {
                        str(cmdr_id): info 
                        for cmdr_id, info in responses.items()
                    } for ticket_id, responses in self.data["command"]["ticket_responses"].items()
                }
            },
            "subscribers": list(self.data["subscribers"]),
            "combat_ready": list(self.data["combat_ready"]),
            "usernames": self.data["usernames"]
        }
    
    def _create_backup(self, data: dict):
        """Create data backup"""
        try:
            with open(Config.BACKUP_FILE, "w", encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Backup creation error: {e}")

# Initialize data manager
data_manager = DataManager()

# --- KEYBOARDS ---
def create_unit_keyboard() -> ReplyKeyboardMarkup:
    """Keyboard for unit members"""
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text=ButtonText.COMBAT_READY),
        KeyboardButton(text=ButtonText.REPORT)
    )
    builder.row(
        KeyboardButton(text=ButtonText.MY_STATUS),
        KeyboardButton(text=ButtonText.HELP)
    )
    return builder.as_markup(resize_keyboard=True)

def create_command_keyboard() -> ReplyKeyboardMarkup:
    """Keyboard for command structure"""
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text=ButtonText.CREATE_MISSION),
        KeyboardButton(text=ButtonText.STATS)
    )
    builder.row(
        KeyboardButton(text=ButtonText.MANAGE_UNITS),
        KeyboardButton(text=ButtonText.SET_CALL_SIGN)
    )
    builder.row(
        KeyboardButton(text=ButtonText.ACTIVE_TICKETS),
        KeyboardButton(text=ButtonText.HELP)
    )
    return builder.as_markup(resize_keyboard=True)

def create_ticket_keyboard(ticket_id: str) -> InlineKeyboardMarkup:
    """Keyboard for ticket processing"""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(
        text="Take Action",
        callback_data=f"take_ticket:{ticket_id}"
    ))
    return builder.as_markup()

def create_response_keyboard(ticket_id: str) -> InlineKeyboardMarkup:
    """Keyboard for ticket response"""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(
        text="✉️ Reply",
        callback_data=f"respond_ticket:{ticket_id}"
    ))
    builder.add(InlineKeyboardButton(
        text="🔒 Close Ticket",
        callback_data=f"close_ticket:{ticket_id}"
    ))
    builder.adjust(2)
    return builder.as_markup()

def create_approval_keyboard(mission_id: str) -> InlineKeyboardMarkup:
    """Keyboard for mission approval"""
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Approve", callback_data=f"approve_mission:{mission_id}")
    builder.button(text="❌ Reject", callback_data=f"reject_mission:{mission_id}")
    builder.adjust(2)
    return builder.as_markup()

# --- MAIN COMMANDS ---
@main_router.message(CommandStart())
async def handle_start(message: Message):
    """Handler for /start command"""
    user_id = message.from_user.id
    data_manager.data["command"]["activity"][user_id] = datetime.now()
    
    # Update username in cache
    if message.from_user.username:
        data_manager.data["usernames"][str(user_id)] = message.from_user.username
        data_manager.save_data()
    
    if is_commander(user_id):
        await message.answer(
            "<b>⚔️ Command Center Activated!</b>\n"
            "Your operational authorities:\n"
            "- Issue strategic directives\n"
            "- Manage units\n"
            "- Monitor mission execution\n"
            "- Assign commanders\n"
            "<i>For the cause!</i>",
            reply_markup=create_command_keyboard()
        )
    else:
        if user_id not in data_manager.data["subscribers"]:
            data_manager.data["subscribers"].add(user_id)
            data_manager.data["units"][UnitType.PRIVATES].add(user_id)
            data_manager.save_data()
            await message.answer(
                "<b>🎖️ Welcome to the CyberGuard!</b>\n"
                "You've been assigned to the unit.\n"
                "Click 'Ready for Action!' to receive missions\n"
                "<i>Unity is strength!</i>",
                reply_markup=create_unit_keyboard()
            )
        else:
            if user_id in data_manager.data["combat_ready"]:
                await message.answer(
                    "ℹ️ <b>You are already active!</b>\n"
                    "Await mission directives.",
                    reply_markup=create_unit_keyboard()
                )
            else:
                await message.answer(
                    "Welcome back! Click 'Ready for Action!' to confirm your status\n"
                    "<i>Together we are unstoppable!</i>",
                    reply_markup=create_unit_keyboard()
                )

# --- USER INFORMATION RETRIEVAL ---
async def update_username_cache(user_id: int, username: Optional[str] = None):
    """Update username cache"""
    if username:
        data_manager.data["usernames"][str(user_id)] = username
        data_manager.save_data()
    elif str(user_id) in data_manager.data["usernames"]:
        return data_manager.data["usernames"][str(user_id)]
    else:
        # No attempts to get username through profile_photos
        data_manager.data["usernames"][str(user_id)] = None
        data_manager.save_data()
    return username or data_manager.data["usernames"].get(str(user_id), f"ID: {user_id}")

def get_username_display(user_id: int) -> str:
    """Get user's display name"""
    username = data_manager.data["usernames"].get(str(user_id))
    if username:
        return f"@{username}" if not username.startswith("@") else username
    return f"ID: {user_id}"

def get_user_medal_and_count(user_id: int) -> str:
    """Returns string with completed mission count and medal"""
    completed = 0
    for mission in data_manager.data["missions"]["archive"].values():
        if mission.get("status") == MissionStatus.COMPLETED and user_id in mission.get("completed_by", set()):
            completed += 1
    if completed >= 100:
        medal = "🏅"
    elif completed >= 50:
        medal = "🥇"
    elif completed >= 25:
        medal = "🥈"
    elif completed >= 10:
        medal = "🥉"
    else:
        medal = ""
    return f"{medal} {completed}" if completed > 0 else "0"

# --- MY STATUS ---
@main_router.message(F.text == ButtonText.MY_STATUS)
async def handle_my_status(message: Message):
    """Handler for 'My Status' command"""
    user_id = message.from_user.id
    data_manager.data["command"]["activity"][user_id] = datetime.now()
    username = await update_username_cache(user_id, message.from_user.username)
    display_name = get_username_display(user_id)
    is_ready = user_id in data_manager.data["combat_ready"]
    call_sign = data_manager.data["command"]["call_signs"].get(user_id, "Not set")
    # Active and completed missions
    active_missions = []
    finished_missions = []
    for mission_id in data_manager.data["missions"]["archive"]:
        mission = data_manager.data["missions"]["archive"][mission_id]
        if user_id in mission.get("completed_by", set()):
            if mission.get("status") == MissionStatus.ACTIVE:
                active_missions.append(mission)
            elif mission.get("status") == MissionStatus.COMPLETED:
                finished_missions.append(mission)
    # Check active tickets
    active_ticket_info = ""
    if user_id in data_manager.data["command"]["user_active_tickets"]:
        ticket_id = data_manager.data["command"]["user_active_tickets"][user_id]
        ticket = data_manager.data["command"]["tickets"].get(ticket_id, {})
        if ticket.get("status") != "closed":
            active_ticket_info = f"Active ticket: {ticket_id} ({ticket.get('status', 'open')})\n"
    status_text = (
        f"👤 <b>Your Status:</b>\n"
        f"Name: {display_name}\n"
        f"Callsign: {call_sign}\n"
        f"Active: {'Yes' if is_ready else 'No'}\n"
        f"{active_ticket_info}"
        f"Active missions: {len(active_missions)}\n"
        f"Completed missions: {len(finished_missions)}\n"
    )
    # Keyboard for mission completion (commanders only)
    builder = InlineKeyboardBuilder()
    if active_missions:
        status_text += "\n<b>Active Missions:</b>\n"
        for m in active_missions:
            status_text += f"- {m.get('name', m['id'])} (ID: {m['id']})\n"
            # Complete button for commanders only
            if is_commander(user_id):
                builder.button(text=f"Complete: {m.get('name', m['id'])}", callback_data=f"finish_mission:{m['id']}")
    if finished_missions:
        status_text += "\n<b>Completed Missions:</b>\n"
        for m in finished_missions:
            status_text += f"- {m.get('name', m['id'])} (ID: {m['id']})\n"
    await message.answer(status_text, reply_markup=builder.as_markup() if builder.buttons else None)

# --- HELP ---
@main_router.message(F.text == ButtonText.HELP)
async def handle_help(message: Message):
    """Handler for /help command"""
    user_id = message.from_user.id
    data_manager.data["command"]["activity"][user_id] = datetime.now()
    is_admin = is_commander(user_id)
    is_centurion = user_id in data_manager.data["units"][UnitType.CENTURIONS]
    help_text = (
        "<b>📚 Command Reference:</b>\n"
        "<b>For All Members:</b>\n"
        "• 'Ready for Action!' – Confirm availability for missions\n"
        "• 'Report to Command' – Create a command report\n"
        "• 'My Status' – View your status and active missions\n"
        "• /help – This guide\n"
    )
    if is_admin or is_centurion:
        help_text += (
            "<b>For Command:</b>\n"
            "• 'Create Mission' – Create a new mission\n"
            "• 'Operation Summary' – View mission and report statistics\n"
            "• 'Manage Units' – Manage unit composition\n"
            "• 'Active Tickets' – View and process reports\n"
            "• 'Set Callsign' – Choose callsign\n"
        )
    await message.answer(help_text)

# --- SET CALLSIGN ---
@main_router.message(F.text == ButtonText.SET_CALL_SIGN)
async def handle_set_call_sign(message: Message):
    """Handler for setting callsign"""
    user_id = message.from_user.id
    data_manager.data["command"]["activity"][user_id] = datetime.now()
    
    # Set state to await new callsign
    data_manager.data["command"]["temp_actions"][user_id] = {
        "action": "set_call_sign",
        "step": "awaiting_input"
    }
    await message.answer(
        "📝 <b>Enter new callsign (max 20 characters):</b>\n"
        "This will be used for identification in the system and visible to other members."
    )

@main_router.message(
    lambda m: m.from_user.id in data_manager.data["command"]["temp_actions"] and
    data_manager.data["command"]["temp_actions"][m.from_user.id].get("action") == "set_call_sign" and
    data_manager.data["command"]["temp_actions"][m.from_user.id].get("step") == "awaiting_input"
)
async def handle_call_sign_input(message: Message):
    """Process callsign input"""
    user_id = message.from_user.id
    new_call_sign = message.text.strip()
    if len(new_call_sign) > Config.MAX_CALL_SIGN_LENGTH:
        await message.answer(f"❌ Callsign too long (max {Config.MAX_CALL_SIGN_LENGTH} characters)")
        return
    
    data_manager.data["command"]["call_signs"][user_id] = new_call_sign
    data_manager.save_data()
    
    await message.answer(
        f"✅ <b>Callsign successfully updated!</b>\n"
        f"New callsign: {new_call_sign}"
    )
    del data_manager.data["command"]["temp_actions"][user_id]

# --- UNIT MANAGEMENT ---
@main_router.message(F.text == ButtonText.MANAGE_UNITS)
async def handle_manage_units(message: Message):
    """Handler for unit management"""
    user_id = message.from_user.id
    if not is_commander(user_id):
        await message.answer("❌ Insufficient permissions!")
        return
    
    data_manager.data["command"]["activity"][user_id] = datetime.now()
    
    # Create unit management keyboard
    builder = InlineKeyboardBuilder()
    for unit_type in UnitType.ALL_TYPES:
        builder.button(
            text=f"{unit_type.capitalize()} ({len(data_manager.data['units'][unit_type] or [])})",
            callback_data=f"manage_units:{unit_type}"
        )
    builder.adjust(1)
    
    await message.answer(
        "⚙️ <b>Unit Management</b>\n"
        "Select unit to manage:",
        reply_markup=builder.as_markup()
    )

@main_router.callback_query(F.data.startswith("manage_units:"))
async def handle_unit_management(callback: CallbackQuery):
    """Handler for unit selection management"""
    unit_type = callback.data.split(":")[1]
    commander_id = callback.from_user.id
    if not is_commander(commander_id):
        await callback.answer("❌ Insufficient permissions!")
        return
    
    data_manager.data["command"]["activity"][commander_id] = datetime.now()
    
    # Create action keyboard
    builder = InlineKeyboardBuilder()
    builder.button(text="Add Member", callback_data=f"add_to_{unit_type}")
    builder.button(text="Remove Member", callback_data=f"remove_from_{unit_type}")
    builder.button(text="View List", callback_data=f"list_{unit_type}")
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"⚙️ <b>Managing {unit_type.capitalize()} Unit</b>",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@main_router.callback_query(
    F.data.startswith("add_to_") |
    F.data.startswith("remove_from_") |
    F.data.startswith("list_")
)
async def handle_unit_actions(callback: CallbackQuery):
    """Handler for unit actions (with pagination)"""
    action = callback.data
    commander_id = callback.from_user.id
    if not is_commander(commander_id):
        await callback.answer("❌ Insufficient permissions!")
        return
    data_manager.data["command"]["activity"][commander_id] = datetime.now()
    # --- Member list pagination ---
    if action.startswith("list_"):
        # Support for list_{unit_type}_page_{n}
        parts = action.split("_")
        unit_type = parts[1]
        page = 0
        if len(parts) >= 4 and parts[2] == "page":
            try:
                page = int(parts[3])
            except Exception:
                page = 0
        members = list(data_manager.data["units"].get(unit_type, set()))
        members.sort()
        PAGE_SIZE = 10
        total = len(members)
        if not members:
            await callback.answer(f"No members in {unit_type} unit.")
            return
        start = page * PAGE_SIZE
        end = start + PAGE_SIZE
        page_members = members[start:end]
        # Get usernames and medals (foundation for future system)
        members_list = []
        for member_id in page_members:
            await update_username_cache(member_id)
            medal_str = get_user_medal_and_count(member_id)
            members_list.append(f"- {get_username_display(member_id)} (ID: {member_id}) — {medal_str}")
        member_list = "\n".join(members_list)
        # Pagination keyboard
        builder = InlineKeyboardBuilder()
        if start > 0:
            builder.button(text="⬅️ Previous", callback_data=f"list_{unit_type}_page_{page-1}")
        if end < total:
            builder.button(text="Next ➡️", callback_data=f"list_{unit_type}_page_{page+1}")
        builder.adjust(2)
        await callback.message.edit_text(
            f"👥 <b>{unit_type.capitalize()} List</b>\n"
            f"Total: {total}\n"
            f"Page {page+1} of {((total-1)//PAGE_SIZE)+1}\n"
            f"{member_list}",
            reply_markup=builder.as_markup() if builder.buttons else None
        )
        await callback.answer()
        return
    # --- Add/Remove member ---
    elif action.startswith("add_to_") or action.startswith("remove_from_"):
        unit_type = action.split("_")[2]
        data_manager.data["command"]["temp_actions"][commander_id] = {
            "action": action,
            "step": "awaiting_input"
        }
        await callback.message.edit_text(
            f"🆔 <b>{'Adding' if 'add' in action else 'Removing'} member from {unit_type.capitalize()}</b>\n"
            "Enter user ID:",
            reply_markup=None
        )
        await callback.answer()

@main_router.message(
    lambda m: m.from_user.id in data_manager.data["command"]["temp_actions"] and
    re.match(r"add_to_|remove_from_", data_manager.data["command"]["temp_actions"][m.from_user.id].get("action", ""))
)
async def handle_user_id_input(message: Message):
    """Process user ID input for unit management"""
    commander_id = message.from_user.id
    action = data_manager.data["command"]["temp_actions"][commander_id]["action"]
    user_id_str = message.text.strip()
    
    try:
        target_user_id = int(user_id_str)
    except ValueError:
        await message.answer("❌ Invalid ID format!")
        return
    
    unit_type = action.split("_")[2]
    is_add = "add" in action
    
    if is_add:
        data_manager.data["units"][unit_type].add(target_user_id)
        await message.answer(
            f"✅ User {target_user_id} added to {unit_type.capitalize()} unit."
        )
    else:
        if target_user_id not in data_manager.data["units"][unit_type]:
            await message.answer("❌ User is not in this unit.")
            return
        
        data_manager.data["units"][unit_type].remove(target_user_id)
        await message.answer(
            f"✅ User {target_user_id} removed from {unit_type.capitalize()} unit."
        )
    
    data_manager.save_data()
    del data_manager.data["command"]["temp_actions"][commander_id]

# --- ACTIVE TICKETS ---
@main_router.message(F.text == ButtonText.ACTIVE_TICKETS)
async def handle_active_tickets(message: Message):
    """Handler for viewing active tickets"""
    user_id = message.from_user.id
    if not is_commander(user_id):
        await message.answer("❌ Insufficient permissions!")
        return
    
    data_manager.data["command"]["activity"][user_id] = datetime.now()
    
    active_tickets = [
        t for t in data_manager.data["command"]["tickets"].values()
        if t["status"] != "closed"
    ]
    
    if not active_tickets:
        await message.answer("ℹ️ No active tickets.")
        return
    
    response = "📋 <b>Active Tickets:</b>\n"
    for ticket in active_tickets:
        response += (
            f"ID: {ticket['id']}\n"
            f"From: {get_username_display(ticket['user_id'])} (ID: {ticket['user_id']})\n"
            f"Status: {ticket['status']}\n"
            f"Created: {datetime.fromisoformat(ticket['created_at']).strftime('%d.%m %H:%M')}\n"
            f"{'-'*20}\n"
        )
    
    # Add commands for viewing specific tickets
    response += "\nTo view a specific ticket, use command: /ticket_123456789"
    await message.answer(response)

# --- БОЕВАЯ СВОДКА ---
@main_router.message(F.text == ButtonText.STATS)
async def handle_stats(message: Message):
    """Handler for operations summary"""
    user_id = message.from_user.id
    if not is_commander(user_id):
        await message.answer("❌ Insufficient permissions!")
        return
    data_manager.data["command"]["activity"][user_id] = datetime.now()
    # Mission statistics
    mission_counts = {
        status: 0 for status in MissionStatus.__dict__.values()
    }
    for mission in data_manager.data["missions"]["archive"].values():
        mission_counts[mission["status"]] += 1
    # Ticket statistics
    open_tickets = sum(1 for t in data_manager.data["command"]["tickets"].values() if t["status"] == "open")
    in_progress_tickets = sum(1 for t in data_manager.data["command"]["tickets"].values() if t["status"] == "in_progress")
    response = (
        "📊 <b>Operations Summary</b>\n"
        "<u>Missions:</u>\n"
        f"- Active: {mission_counts[MissionStatus.ACTIVE]}\n"
        f"- Completed: {mission_counts[MissionStatus.COMPLETED]}\n"
        f"- Pending: {mission_counts[MissionStatus.PENDING]}\n"
        f"- Rejected: {mission_counts[MissionStatus.REJECTED]}\n"
        "<u>Tickets:</u>\n"
        f"- Waiting: {open_tickets}\n"
        f"- In Progress: {in_progress_tickets}\n"
        "<u>Units:</u>\n"
        f"- {UnitType.CENTURIONS.capitalize()}: {len(data_manager.data['units'][UnitType.CENTURIONS])}\n"
        f"- {UnitType.DECURIONS.capitalize()}: {len(data_manager.data['units'][UnitType.DECURIONS])}\n"
        f"- {UnitType.PRIVATES.capitalize()}: {len(data_manager.data['units'][UnitType.PRIVATES])}\n"
        "\n<b>Active Missions:</b>"
    )
    # Add list of active missions with names and completion buttons
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()
    active_missions = []
    for mission_id in data_manager.data["missions"]["active"]:
        mission = data_manager.data["missions"]["archive"].get(mission_id, {})
        mission_name = mission.get("name", mission_id)
        status = mission.get('status', 'unknown')
        active_missions.append(f"- {mission_name} ({status})")
        if status == MissionStatus.ACTIVE:
            builder.button(text=f"Complete: {mission_name}", callback_data=f"finish_mission:{mission_id}")
    if active_missions:
        response += "\n" + "\n".join(active_missions)
    else:
        response += "\nNo active missions"
    await message.answer(response, reply_markup=builder.as_markup() if builder.buttons else None)

# --- TICKET SYSTEM ---
@main_router.message(Command(commands=["ticket"]))
async def handle_ticket_command(message: Message, command: Command):
    """View ticket by command /ticket_123"""
    if not command.args:
        await message.answer("❌ Please specify the ticket ID")
        return
    
    ticket_id = command.args.strip()
    if ticket_id not in data_manager.data["command"]["tickets"]:
        await message.answer("❌ Ticket not found!")
        return
    
    ticket = data_manager.data["command"]["tickets"][ticket_id]
    user_id = message.from_user.id
    
    # Check if user has permission to view the ticket
    if not is_commander(user_id) and user_id != ticket["user_id"] and user_id != ticket.get("assigned_to"):
        await message.answer("❌ Insufficient permissions!")
        return
    
    # Get user information
    display_name = get_username_display(ticket["user_id"])
    
    details = (
        f"📋 <b>Ticket Details {ticket_id}</b>\n"
        f"From: {display_name} (ID: {ticket['user_id']})\n"
        f"Status: {ticket['status']}\n"
        f"Created: {datetime.fromisoformat(ticket['created_at']).strftime('%d.%m %H:%M')}\n"
        f"\n<b>Ticket History:</b>\n"
    )
    # Collect all messages and responses in chronological order
    history = []
    for msg in ticket.get("messages", []):
        history.append({
            "type": "user",
            "text": msg if isinstance(msg, str) else msg.get("text", str(msg)),
            "time": ticket.get("created_at"),
        })
    for resp in ticket.get("responses", []):
        history.append({
            "type": "commander",
            "text": resp["text"],
            "time": resp.get("timestamp"),
            "commander_id": resp.get("commander_id"),
        })
    # Сортируем по времени (если есть timestamp)
    def get_time(x):
        try:
            return datetime.fromisoformat(x["time"]) if x["time"] else datetime.min
        except Exception:
            return datetime.min
    history.sort(key=get_time)
    if not history:
        details += "No messages in this ticket.\n"
    else:
        for h in history:
            if h["type"] == "user":
                details += f"👤 User: {h['text']}\n"
            else:
                call_sign = data_manager.data["command"]["call_signs"].get(h.get("commander_id"), f"Commander-{h.get('commander_id')}")
                details += f"🛡️ {call_sign}: {h['text']}\n"
    # Add keyboard if user is a commander
    if is_commander(user_id):
        keyboard = create_response_keyboard(ticket_id)
        await message.answer(details, reply_markup=keyboard)
    else:
        await message.answer(details)

# --- MISSION SYSTEM ---
async def distribute_mission(mission: dict, targets: list):
    """Distribute mission to units"""
    mission_id = mission["id"]
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Completed", callback_data=f"complete_mission:{mission_id}")
    
    # Use mission name and MarkdownV2, no text escaping
    mission_name = mission.get('name', mission_id)
    content = mission.get('content', '')
    parse_mode = "MarkdownV2"
    # Message without HTML, without "Mission status" and other extra lines
    for unit_type in targets:
        for user_id in data_manager.data["units"][unit_type]:
            try:
                await bot.send_message(
                    user_id,
                    f"⚡ *Mission: {mission_name}*\n{content}",
                    reply_markup=builder.as_markup(),
                    parse_mode=parse_mode
                )
            except TelegramForbiddenError:
                logger.warning(f"User {user_id} blocked the bot. Removing from database.")
                remove_user_from_database(user_id)
            except Exception as e:
                logger.error(f"Error sending mission to {user_id}: {e}")

# --- CREATE MISSION WITH NAME ---
@main_router.message(F.text == ButtonText.CREATE_MISSION)
async def handle_create_mission(message: Message):
    """Create new mission"""
    user_id = message.from_user.id
    # Decurions can only create missions for privates
    is_admin = is_commander(user_id)
    is_centurion = user_id in data_manager.data["units"][UnitType.CENTURIONS]
    is_decurion = user_id in data_manager.data["units"][UnitType.DECURIONS]
    if not (is_admin or is_centurion or is_decurion):
        await message.answer("❌ Insufficient permissions!")
        return
    data_manager.data["command"]["activity"][user_id] = datetime.now()
    # Create mission type selection keyboard
    builder = InlineKeyboardBuilder()
    if is_admin or is_centurion:
        builder.button(text="For All Units", callback_data="mission_type:all")
        builder.button(text="For Decurions Only", callback_data="mission_type:decurions")
    # Decurions and above can create for privates
    if is_admin or is_centurion or is_decurion:
        builder.button(text="For Privates Only", callback_data="mission_type:privates")
    builder.adjust(1)
    await message.answer(
        "⚡ <b>Create New Mission</b>\n"
        "Select mission type:",
        reply_markup=builder.as_markup()
    )

@main_router.callback_query(F.data.startswith("mission_type:"))
async def handle_mission_type(callback: CallbackQuery):
    """Handle mission type selection"""
    mission_type = callback.data.split(":")[1]
    user_id = callback.from_user.id
    
    # Save temporary data
    data_manager.data["command"]["temp_missions"][user_id] = {
        "type": mission_type,
        "step": "awaiting_name"
    }
    
    await callback.message.edit_text(
        f"⚡ <b>Create New Mission</b>\n"
        f"Type: {mission_type.replace('_', ' ').title()}\n"
        f"Enter mission name (max {Config.MAX_MISSION_NAME_LENGTH} characters):"
    )
    await callback.answer()

@main_router.message(
    lambda m: m.from_user.id in data_manager.data["command"]["temp_missions"] and
    data_manager.data["command"]["temp_missions"][m.from_user.id]["step"] == "awaiting_name"
)
async def handle_mission_name(message: Message):
    """Process mission name input"""
    user_id = message.from_user.id
    mission_data = data_manager.data["command"]["temp_missions"][user_id]
    mission_type = mission_data["type"]
    mission_name = message.text.strip()
    
    if len(mission_name) > Config.MAX_MISSION_NAME_LENGTH:
        await message.answer(f"❌ Mission name too long (max {Config.MAX_MISSION_NAME_LENGTH} characters)")
        return
    
    # Update step
    data_manager.data["command"]["temp_missions"][user_id]["step"] = "awaiting_content"
    data_manager.data["command"]["temp_missions"][user_id]["name"] = mission_name
    
    await message.answer(
        f"⚡ <b>Create New Mission</b>\n"
        f"Name: {mission_name}\n"
        "Enter mission content:"
    )

@main_router.message(
    lambda m: m.from_user.id in data_manager.data["command"]["temp_missions"] and
    data_manager.data["command"]["temp_missions"][m.from_user.id]["step"] == "awaiting_content"
)
async def handle_mission_content(message: Message):
    """Process mission content"""
    user_id = message.from_user.id
    mission_data = data_manager.data["command"]["temp_missions"][user_id]
    mission_type = mission_data["type"]
    mission_content = message.text
    mission_name = mission_data.get("name", "Untitled")
    is_admin = is_commander(user_id)
    is_centurion = int(user_id) in set(int(x) for x in data_manager.data["units"][UnitType.CENTURIONS])
    is_decurion = int(user_id) in set(int(x) for x in data_manager.data["units"][UnitType.DECURIONS])
    # Determine target group
    if mission_type == "all":
        if not (is_admin or is_centurion):
            await message.answer("❌ Insufficient permissions to create this type of mission!")
            return
        targets = UnitType.ALL_TYPES
    elif mission_type == "decurions":
        if not (is_admin or is_centurion):
            await message.answer("❌ Insufficient permissions to create this type of mission!")
            return
        targets = [UnitType.DECURIONS, UnitType.CENTURIONS]
    else:  # privates
        if not (is_admin or is_centurion or is_decurion):
            await message.answer("❌ Insufficient permissions to create this type of mission!")
            return
        targets = [UnitType.PRIVATES]
    # Create mission
    mission_id = f"mission_{user_id}_{int(datetime.now().timestamp())}"
    mission = {
        "id": mission_id,
        "creator": user_id,
        "type": mission_type,
        "name": mission_name,
        "content": mission_content,
        "status": MissionStatus.PENDING,
        "created_at": datetime.now().isoformat(),
        "completed_by": set()
    }
    # For private missions, approval required if not admin/centurion
    if mission_type == "privates" and not (is_admin or is_centurion):
        mission["status"] = MissionStatus.PENDING
        approval_keyboard = create_approval_keyboard(mission_id)
        # Send for approval to centurions
        for centurion_id in data_manager.data["units"][UnitType.CENTURIONS]:
            try:
                msg = await bot.send_message(
                    centurion_id,
                    f"🔐 <b>Mission Approval Request</b>\n"
                    f"Creator: {user_id}\n"
                    f"Type: For Privates\n"
                    f"Name: {mission_name}\n"
                    f"Content:\n{mission_content}",
                    reply_markup=approval_keyboard
                )
                # Save message ID for updates
                data_manager.data["command"]["ticket_responses"].setdefault(mission_id, {})[centurion_id] = {
                    "message_id": msg.message_id,
                    "chat_id": centurion_id
                }
            except Exception as e:
                logger.error(f"Error sending approval request to {centurion_id}: {e}")
        await message.answer(
            "🕒 <b>Mission sent for command approval</b>\n"
            "You will be notified after review."
        )
    else:
        # No approval needed
        mission["status"] = MissionStatus.ACTIVE
        await distribute_mission(mission, targets)
        await message.answer(
            f"✅ <b>Mission \"{mission_name}\" launched!</b>\n"
            f"Reach: {len(targets)} units"
        )
    # Save mission
    data_manager.data["missions"]["active"].append(mission_id)
    data_manager.data["missions"]["archive"][mission_id] = mission
    del data_manager.data["command"]["temp_missions"][user_id]
    data_manager.save_data()

# --- MISSION APPROVAL/REJECTION ---
@main_router.callback_query(F.data.startswith("approve_mission:"))
async def handle_approve_mission(callback: CallbackQuery):
    """Handle mission approval"""
    mission_id = callback.data.split(":")[1]
    commander_id = callback.from_user.id
    if mission_id not in data_manager.data["missions"]["archive"]:
        await callback.answer("❌ Mission not found!")
        return
    mission = data_manager.data["missions"]["archive"][mission_id]
    if mission["status"] != MissionStatus.PENDING:
        await callback.answer("❌ Mission already processed!")
        return
    # Update status
    mission["status"] = MissionStatus.ACTIVE
    mission["approved_by"] = commander_id
    mission["approved_at"] = datetime.now().isoformat()
    # Distribute mission
    mission_type = mission["type"]
    if mission_type == "all":
        targets = UnitType.ALL_TYPES
    elif mission_type == "decurions":
        targets = [UnitType.DECURIONS, UnitType.CENTURIONS]
    else:  # privates
        targets = [UnitType.PRIVATES]
    await distribute_mission(mission, targets)
    # Notify creator
    creator_id = mission["creator"]
    try:
        await bot.send_message(
            creator_id,
            f"✅ <b>Your mission \"{mission.get('name', mission_id)}\" has been approved!</b>\n"
            "It has been distributed to the appropriate units."
        )
    except TelegramForbiddenError:
        logger.warning(f"User {creator_id} blocked the bot. Removing from database.")
        remove_user_from_database(creator_id)
    except Exception as e:
        logger.error(f"Error notifying creator {creator_id}: {e}")
    await callback.answer("✅ Mission approved and launched!")
    data_manager.save_data()

@main_router.callback_query(F.data.startswith("reject_mission:"))
async def handle_reject_mission(callback: CallbackQuery):
    """Handle mission rejection"""
    mission_id = callback.data.split(":")[1]
    commander_id = callback.from_user.id
    
    if mission_id not in data_manager.data["missions"]["archive"]:
        await callback.answer("❌ Mission not found!")
        return
    
    mission = data_manager.data["missions"]["archive"][mission_id]
    
    if mission["status"] != MissionStatus.PENDING:
        await callback.answer("❌ Mission already processed!")
        return
    
    # Update status
    mission["status"] = MissionStatus.REJECTED
    mission["rejected_by"] = commander_id
    mission["rejected_at"] = datetime.now().isoformat()
    
    # Notify creator
    creator_id = mission["creator"]
    try:
        await bot.send_message(
            creator_id,
            f"❌ <b>Your mission \"{mission.get('name', mission_id)}\" has been rejected.</b>\n"
            "Command has determined this mission should not proceed."
        )
    except TelegramForbiddenError:
        logger.warning(f"User {creator_id} blocked the bot. Removing from database.")
        remove_user_from_database(creator_id)
    except Exception as e:
        logger.error(f"Error notifying creator {creator_id}: {e}")
    
    await callback.answer("❌ Mission rejected.")
    data_manager.save_data()

# --- TICKET SYSTEM ---
@main_router.message(F.text == ButtonText.REPORT)
async def handle_report_start(message: Message):
    """Start creating a report"""
    user_id = message.from_user.id
    data_manager.data["command"]["activity"][user_id] = datetime.now()
    
    # Check for active ticket
    if user_id in data_manager.data["command"]["user_active_tickets"]:
        ticket_id = data_manager.data["command"]["user_active_tickets"][user_id]
        ticket = data_manager.data["command"]["tickets"].get(ticket_id)
        if ticket and ticket["status"] != "closed":
            await message.answer(
                "ℹ️ <b>You already have an active ticket!</b>\n"
                f"ID: {ticket_id}\n"
                f"Status: {ticket.get('status', 'unknown')}\n"
                "You can continue the conversation in this chat."
            )
            return
    
    # Set state to await ticket text
    data_manager.data["command"]["temp_actions"][user_id] = {
        "action": "create_ticket",
        "step": "awaiting_text"
    }
    
    await message.answer(
        "📝 <b>Enter your report text:</b>\n"
        "Please describe the issue or question in detail."
    )

@main_router.message(
    lambda m: m.from_user.id in data_manager.data["command"]["temp_actions"] and
    data_manager.data["command"]["temp_actions"][m.from_user.id].get("action") == "create_ticket" and
    data_manager.data["command"]["temp_actions"][m.from_user.id].get("step") == "awaiting_text"
)
async def handle_report_text(message: Message):
    """Process report text"""
    user_id = message.from_user.id
    report_text = message.text
    
    # Create ticket
    ticket_id = f"ticket_{user_id}_{int(datetime.now().timestamp())}"
    ticket = {
        "id": ticket_id,
        "user_id": user_id,
        "text": report_text,
        "status": "open",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "assigned_to": None,
        "messages": [report_text],
        "responses": []
    }
    
    data_manager.data["command"]["tickets"][ticket_id] = ticket
    data_manager.data["command"]["user_active_tickets"][user_id] = ticket_id
    
    # Send to commanders
    commanders = set(data_manager.data["units"][UnitType.CENTURIONS]) | Config.ADMIN_IDS
    
    for commander_id in commanders:
        try:
            msg = await bot.send_message(
                commander_id,
                f"🚨 <b>New report from member (ID: {user_id}):</b>\n"
                f"{report_text}\n"
                f"ID: {ticket_id}",
                reply_markup=create_ticket_keyboard(ticket_id)
            )
            # Save message ID for updates
            data_manager.data["command"]["ticket_responses"].setdefault(ticket_id, {})[commander_id] = {
                "message_id": msg.message_id,
                "chat_id": commander_id
            }
        except Exception as e:
            logger.error(f"Error sending to commander {commander_id}: {e}")
    
    await message.answer(
        "✅ <b>Your report has been registered!</b>\n"
        f"ID: {ticket_id}\n"
        "Command will contact you shortly.\n"
        "<i>You can continue writing in this chat to add to your report.</i>"
    )
    
    # Очистка временных данных
    if user_id in data_manager.data["command"]["temp_actions"]:
        del data_manager.data["command"]["temp_actions"][user_id]
    
    data_manager.save_data()

# --- ОБРАБОТЧИК ЗАВЕРШЕНИЯ МИССИЙ ---
@main_router.callback_query(F.data.startswith("complete_mission:"))
async def handle_mission_complete(callback: CallbackQuery):
    """Обработка завершения миссии"""
    mission_id = callback.data.split(":")[1]
    user_id = callback.from_user.id
    if mission_id not in data_manager.data["missions"]["archive"]:
        await callback.answer("❌ Миссия не найдена!")
        return
    mission = data_manager.data["missions"]["archive"][mission_id]
    if mission["status"] != MissionStatus.ACTIVE:
        await callback.answer("❌ Миссия не активна!")
        return
    # Проверка повторного выполнения
    if user_id in mission.get("completed_by", set()):
        await callback.answer("ℹ️ Вы уже отметили выполнение этой миссии!")
        return
    # Отметка выполнения
    if "completed_by" not in mission or not isinstance(mission["completed_by"], set):
        mission["completed_by"] = set(mission.get("completed_by", []))
    mission["completed_by"].add(user_id)
    data_manager.data["missions"]["archive"][mission_id] = mission
    # Проверяем, все ли выполнили миссию
    mission_type = mission["type"]
    if mission_type == "all":
        targets = UnitType.ALL_TYPES
    elif mission_type == "decurions":
        targets = [UnitType.DECURIONS, UnitType.CENTURIONS]
    else:  # privates
        targets = [UnitType.PRIVATES]
    total_targets = sum(len(data_manager.data["units"][t]) for t in targets)
    completed_count = len(mission.get("completed_by", set()))
    if completed_count == total_targets:
        # All users completed the mission, send notification
        completion_message = (
            f"✅ <b>Mission \"{mission.get('name', mission_id)}\" completed!</b>\n"
            "Thank you for your participation"
        )
        for completed_user_id in mission.get("completed_by", set()):
            try:
                await bot.send_message(
                    completed_user_id,
                    completion_message
                )
            except TelegramForbiddenError:
                logger.warning(f"User {completed_user_id} blocked the bot. Removing from database.")
                remove_user_from_database(completed_user_id)
            except Exception as e:
                logger.error(f"Error sending completion notification to {completed_user_id}: {e}")
    await callback.answer("✅ Your report has been accepted! Thank you for your participation.")
    data_manager.save_data()

# --- СИСТЕМА ОБРАЩЕНИЙ ---
@main_router.callback_query(F.data.startswith("take_ticket:"))
async def handle_take_ticket(callback: CallbackQuery):
    """Принятие обращения в работу"""
    try:
        ticket_id = callback.data.split(":")[1]
        commander_id = callback.from_user.id
        
        if ticket_id not in data_manager.data["command"]["tickets"]:
            await callback.answer("❌ Обращение не найдено!")
            return
        
        ticket = data_manager.data["command"]["tickets"][ticket_id]
        
        if ticket["status"] != "open":
            await callback.answer("❌ Обращение уже в работе!")
            return
        
        # Обновление статуса
        ticket["status"] = "in_progress"
        ticket["assigned_to"] = commander_id
        ticket["assigned_at"] = datetime.now().isoformat()
        ticket["updated_at"] = datetime.now().isoformat()
        
        # Получение позывного
        call_sign = data_manager.data["command"]["call_signs"].get(
            commander_id, 
            f"Командир-{commander_id}"
        )
        
        # Уведомление бойца
        user_id = ticket["user_id"]
        try:
            await bot.send_message(
                user_id,
                f"ℹ️ <b>Ваше обращение принято в работу!</b>\n"
                f"Ответственный: {call_sign}\n"
                f"ID обращения: {ticket_id}"
            )
        except TelegramForbiddenError:
            logger.warning(f"Пользователь {user_id} заблокировал бота. Удаляем из базы.")
            remove_user_from_database(user_id)
        except Exception as e:
            logger.error(f"Ошибка уведомления бойца {user_id}: {e}")
        
        # Update message for supervisors (current message only)
        try:
            await callback.message.edit_reply_markup(
                reply_markup=create_response_keyboard(ticket_id)
            )
            # Update ticket_responses record for this supervisor
            data_manager.data["command"]["ticket_responses"].setdefault(ticket_id, {})[supervisor_id] = {
                "message_id": callback.message.message_id,
                "chat_id": supervisor_id
            }
        except Exception as e:
            logger.error(f"Error updating message: {e}")
        
        await callback.answer("✅ You have taken the ticket")
        data_manager.save_data()
    except Exception as e:
        logger.error(f"Error in handle_take_ticket: {e}")
        await callback.answer("❌ An error occurred")

# --- TICKET CLOSURE HANDLER ---
@main_router.callback_query(F.data.startswith("close_ticket:"))
async def handle_close_ticket(callback: CallbackQuery):
    """Close the ticket"""
    ticket_id = callback.data.split(":")[1]
    user_id = callback.from_user.id
    
    if ticket_id not in data_manager.data["command"]["tickets"]:
        await callback.answer("❌ Ticket not found!")
        return
    
    ticket = data_manager.data["command"]["tickets"][ticket_id]
    
    # Check closure permissions
    if not is_commander(user_id) and user_id != ticket["user_id"] and user_id != ticket.get("assigned_to"):
        await callback.answer("❌ Insufficient permissions to close the ticket!")
        return
    
    # Update status
    ticket["status"] = "closed"
    ticket["closed_at"] = datetime.now().isoformat()
    ticket["updated_at"] = datetime.now().isoformat()
    
    # Notify all parties
    user_id_ticket = ticket["user_id"]
    try:
        await bot.send_message(
            user_id_ticket,
            f"✅ <b>Your ticket {ticket_id} has been closed.</b>\n"
            "If your issue is resolved, thank you for your cooperation!\n"
            "If you need further assistance, you can create a new ticket."
        )
    except TelegramForbiddenError:
        logger.warning(f"User {user_id_ticket} blocked the bot. Removing from database.")
        remove_user_from_database(user_id_ticket)
    except Exception as e:
        logger.error(f"Error notifying user {user_id_ticket}: {e}")
    
    # Notify supervisors
    for supervisor_id in data_manager.data["command"]["ticket_responses"].get(ticket_id, {}):
        try:
            # Remove keyboard markup from messages
            await bot.edit_message_reply_markup(
                chat_id=supervisor_id,
                message_id=data_manager.data["command"]["ticket_responses"][ticket_id][supervisor_id]["message_id"],
                reply_markup=None
            )
        except Exception as e:
            logger.error(f"Error updating supervisor message {supervisor_id}: {e}")
    
    # Clear active references
    if user_id_ticket in data_manager.data["command"]["user_active_tickets"]:
        del data_manager.data["command"]["user_active_tickets"][user_id_ticket]
    
    data_manager.save_data()
    await callback.answer("🔒 Ticket closed")

# --- ФОНОВЫЕ ЗАДАЧИ ---
async def auto_save_task():
    """Background task for auto-saving data"""
    while True:
        await asyncio.sleep(Config.AUTO_SAVE_INTERVAL)
        data_manager.save_data()

async def cleanup_tickets():
    """Cleanup of expired tickets"""
    while True:
        await asyncio.sleep(3600)  # Every hour
        now = datetime.now()
        expired_tickets = []
        
        for ticket_id, ticket in data_manager.data["command"]["tickets"].items():
            if ticket["status"] == "closed":
                continue
            
            updated_at = datetime.fromisoformat(ticket["updated_at"])
            if (now - updated_at).total_seconds() > Config.TICKET_TIMEOUT * 3600:
                expired_tickets.append(ticket_id)
        
        for ticket_id in expired_tickets:
            ticket = data_manager.data["command"]["tickets"][ticket_id]
            user_id = ticket["user_id"]
            
            # Notify the user
            try:
                await bot.send_message(
                    user_id,
                    f"ℹ️ <b>Your ticket {ticket_id} has been closed due to inactivity.</b>\n"
                    "If your issue is not resolved, you can create a new ticket."
                )
            except TelegramForbiddenError:
                logger.warning(f"User {user_id} blocked the bot. Removing from database.")
                remove_user_from_database(user_id)
            except Exception as e:
                logger.error(f"Error notifying user {user_id}: {e}")
            
            # Close the ticket
            ticket["status"] = "closed"
        
        if expired_tickets:
            data_manager.save_data()
            logger.info(f"Closed {len(expired_tickets)} expired tickets")

# --- SYSTEM STARTUP ---
async def on_startup():
    """Actions performed during system startup"""
    logger.info("Starting Cyber Guard system")
    
    # Launch background tasks
    asyncio.create_task(auto_save_task())
    asyncio.create_task(cleanup_tickets())
    
    # Notify administrators
    for admin_id in Config.ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                "🟢 <b>Cyber Guard system is now online!</b>\n"
                f"Version: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                f"Active tickets: {sum(1 for t in data_manager.data['command']['tickets'].values() if t['status'] != 'closed')}"
            )
        except Exception as e:
            logger.error(f"Error notifying administrator {admin_id}: {e}")

async def on_shutdown():
    """Actions performed during system shutdown"""
    logger.info("System shutdown initiated")
    data_manager.save_data()

# --- MISSION SYSTEM ---
@main_router.message(F.text == ButtonText.COMBAT_READY)
async def handle_combat_ready(message: Message):
    """Handler for readiness confirmation"""
    user_id = message.from_user.id
    data_manager.data["command"]["activity"][user_id] = datetime.now()
    
    if user_id in data_manager.data["combat_ready"]:
        await message.answer(
            "ℹ️ <b>You are already on duty!</b>\n"
            "Await mission assignments.",
            reply_markup=create_unit_keyboard()
        )
    else:
        data_manager.data["combat_ready"].add(user_id)
        data_manager.save_data()
        try:
            await bot.send_message(
                user_id,
                f"✅ <b>Ready status confirmed!</b>\n"
                "Await mission assignments.",
                reply_markup=create_unit_keyboard()
            )
        except TelegramForbiddenError:
            logger.warning(f"User {user_id} blocked the bot. Removing from database.")
            remove_user_from_database(user_id)
        except Exception as e:
            logger.error(f"Error sending message to user {user_id}: {e}")
        await message.answer(
            "✅ <b>Ready status confirmed!</b>\n"
            "Await mission assignments.",
            reply_markup=create_unit_keyboard()
        )

# --- TICKET SYSTEM ---
@main_router.callback_query(F.data.startswith("respond_ticket:"))
async def handle_respond_ticket(callback: CallbackQuery):
    """Handle 'Reply' button press on a ticket"""
    ticket_id = callback.data.split(":")[1]
    moderator_id = callback.from_user.id
    if ticket_id not in data_manager.data["command"]["tickets"]:
        await callback.answer("❌ Ticket not found!")
        return
    ticket = data_manager.data["command"]["tickets"][ticket_id]
    # Check permissions
    if not is_commander(moderator_id):
        await callback.answer("❌ Insufficient permissions!")
        return
    # Save response waiting state
    data_manager.data["command"]["temp_actions"][moderator_id] = {
        "action": f"respond_ticket_{ticket_id}",
        "step": "awaiting_text"
    }
    await callback.message.answer(
        f"✉️ <b>Enter your response for ticket {ticket_id}:</b>"
    )
    await callback.answer()

@main_router.message(
    lambda m: m.from_user.id in data_manager.data["command"]["temp_actions"] and
    data_manager.data["command"]["temp_actions"][m.from_user.id]["action"].startswith("respond_ticket_") and
    data_manager.data["command"]["temp_actions"][m.from_user.id]["step"] == "awaiting_text"
)
async def handle_respond_ticket_text(message: Message):
    """Handle moderator's response text for a ticket"""
    moderator_id = message.from_user.id
    action = data_manager.data["command"]["temp_actions"][moderator_id]["action"]
    ticket_id = action.replace("respond_ticket_", "")
    if ticket_id not in data_manager.data["command"]["tickets"]:
        await message.answer("❌ Ticket not found!")
        del data_manager.data["command"]["temp_actions"][moderator_id]
        return
    ticket = data_manager.data["command"]["tickets"][ticket_id]
    response_text = message.text.strip()
    # Add response
    ticket.setdefault("responses", []).append({
        "moderator_id": moderator_id,
        "text": response_text,
        "timestamp": datetime.now().isoformat()
    })
    ticket["updated_at"] = datetime.now().isoformat()
    # Notify the user
    user_id = ticket["user_id"]
    try:
        await bot.send_message(
            user_id,
            f"✉️ <b>Response to your ticket {ticket_id}:</b>\n{response_text}"
        )
    except TelegramForbiddenError:
        logger.warning(f"User {user_id} blocked the bot. Removing from database.")
        remove_user_from_database(user_id)
    except Exception as e:
        logger.error(f"Error sending response to user {user_id}: {e}")
    await message.answer("✅ Response has been sent.")
    del data_manager.data["command"]["temp_actions"][moderator_id]
    data_manager.save_data()

# --- Mission completion button for moderators ---
@main_router.callback_query(F.data.startswith("finish_mission:"))
async def handle_finish_mission(callback: CallbackQuery):
    mission_id = callback.data.split(":")[1]
    user_id = callback.from_user.id
    user_is_commander = is_commander(user_id)
    if not user_is_commander:
        await callback.answer("❌ Insufficient permissions!")
        return
    if mission_id not in data_manager.data["missions"]["archive"]:
        await callback.answer("❌ Mission not found!")
        return
    mission = data_manager.data["missions"]["archive"][mission_id]
    if mission["status"] == MissionStatus.COMPLETED:
        await callback.answer("Mission is already completed!")
        return
    mission["status"] = MissionStatus.COMPLETED
    mission["completed_at"] = datetime.now().isoformat()
    data_manager.data["missions"]["archive"][mission_id] = mission
    # Notify only those who completed the mission
    for uid in mission.get("completed_by", set()):
        try:
            await bot.send_message(uid, f"✅ <b>Mission '{mission.get('name', mission_id)}' has been completed by moderator!</b>")
        except TelegramForbiddenError:
            logger.warning(f"User {uid} blocked the bot. Removing from database.")
            remove_user_from_database(uid)
        except Exception as e:
            logger.error(f"Error notifying user {uid} about completion: {e}")
    data_manager.save_data()
    await callback.answer("Mission completed!")

@main_router.message(Command(commands=["finish_mission"]))
async def handle_finish_mission_command(message: Message, command: CommandObject):
    user_id = message.from_user.id
    if not is_commander(user_id) and not is_commander(user_id):
        await message.answer("❌ Insufficient permissions!")
        return
    if not command.args:
        await message.answer("Please specify mission ID: /finish_mission <id>")
        return
    mission_id = command.args.strip()
    if mission_id not in data_manager.data["missions"]["archive"]:
        await message.answer("Mission not found!")
        return
    mission = data_manager.data["missions"]["archive"][mission_id]
    if mission["status"] == MissionStatus.COMPLETED:
        await message.answer("Mission is already completed!")
        return
    mission["status"] = MissionStatus.COMPLETED
    mission["completed_at"] = datetime.now().isoformat()
    data_manager.data["missions"]["archive"][mission_id] = mission
    # Notify only those who completed the mission
    for uid in mission.get("completed_by", set()):
        try:
            await bot.send_message(uid, f"✅ <b>Mission '{mission.get('name', mission_id)}' has been completed by moderator!</b>")
        except TelegramForbiddenError:
            logger.warning(f"User {uid} blocked the bot. Removing from database.")
            remove_user_from_database(uid)
        except Exception as e:
            logger.error(f"Error notifying user {uid} about completion: {e}")
    data_manager.save_data()
    await message.answer("Mission completed!")

# --- SYSTEM STARTUP ---
async def main():
    """Main startup function"""
    # Connect event handlers
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    # Start the bot
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Bot is ready to work")
    await dp.start_polling(bot)

# --- Ticket dialog: user section ---
@main_router.message(
    lambda m: (
        m.from_user.id in data_manager.data["command"]["user_active_tickets"] and
        data_manager.data["command"]["tickets"].get(
            data_manager.data["command"]["user_active_tickets"][m.from_user.id], {}
        ).get("status") == "in_progress" and
        not (m.text and m.text.strip().startswith("/close"))
    )
)
async def handle_ticket_dialog_user(message: Message):
    """Forward user messages to the assigned moderator for the ticket"""
    user_id = message.from_user.id
    ticket_id = data_manager.data["command"]["user_active_tickets"][user_id]
    ticket = data_manager.data["command"]["tickets"][ticket_id]
    moderator_id = ticket.get("assigned_to")
    if not moderator_id:
        await message.answer("⏳ Your ticket is waiting to be assigned to a moderator.")
        return
    # Add message to ticket history
    ticket.setdefault("messages", []).append({"from": "user", "text": message.text, "timestamp": datetime.now().isoformat()})
    ticket["updated_at"] = datetime.now().isoformat()
    data_manager.save_data()
    # Forward to moderator
    try:
        await bot.send_message(moderator_id, f"💬 <b>User message for ticket {ticket_id}:</b>\n{message.text}")
    except Exception as e:
        logger.error(f"Error forwarding message to moderator {moderator_id}: {e}")
    # Don't respond to user to avoid interrupting the dialog

# --- Ticket dialog: moderator section ---
@main_router.message(
    lambda m: any(
        t.get("assigned_to") == m.from_user.id and t["status"] == "in_progress"
        for t in data_manager.data["command"]["tickets"].values()
    ) and not (m.text and m.text.strip().startswith("/close"))
)
async def handle_ticket_dialog_admin(message: Message):
    """Forward moderator's messages to the user for the ticket"""
    moderator_id = message.from_user.id
    # Find ticket where this moderator is assigned_to and status is in_progress
    ticket = next((t for t in data_manager.data["command"]["tickets"].values() if t.get("assigned_to") == moderator_id and t["status"] == "in_progress"), None)
    if not ticket:
        return
    user_id = ticket["user_id"]
    # Add message to ticket history
    ticket.setdefault("responses", []).append({"moderator_id": moderator_id, "text": message.text, "timestamp": datetime.now().isoformat()})
    ticket["updated_at"] = datetime.now().isoformat()
    data_manager.save_data()
    # Forward to user
    try:
        await bot.send_message(user_id, f"💬 <b>Response for ticket {ticket['id']}:</b>\n{message.text}")
    except Exception as e:
        logger.error(f"Error forwarding message to user {user_id}: {e}")
    # Don't respond to moderator to avoid interrupting the dialog

# --- Close ticket with /close command ---
@main_router.message(Command(commands=["close"]))
async def handle_close_ticket_command(message: Message, command: CommandObject):
    user_id = message.from_user.id
    # Check if user has an active ticket or is assigned to a ticket
    ticket_id = None
    ticket = None
    # For user
    if user_id in data_manager.data["command"]["user_active_tickets"]:
        ticket_id = data_manager.data["command"]["user_active_tickets"][user_id]
        ticket = data_manager.data["command"]["tickets"].get(ticket_id)
    # For moderator
    if not ticket:
        for t in data_manager.data["command"]["tickets"].values():
            if t.get("assigned_to") == user_id and t["status"] == "in_progress":
                ticket_id = t["id"]
                ticket = t
                break
    if not ticket or ticket["status"] == "closed":
        await message.answer("No active ticket to close.")
        return
    # Close the ticket
    ticket["status"] = "closed"
    ticket["closed_at"] = datetime.now().isoformat()
    ticket["updated_at"] = datetime.now().isoformat()
    data_manager.save_data()
    # Notify both parties
    try:
        await bot.send_message(ticket["user_id"], f"✅ <b>Your ticket {ticket_id} has been closed.</b>\nThank you for the dialog!")
    except Exception as e:
        logger.error(f"Error notifying user about ticket closure: {e}")
    if ticket.get("assigned_to"):
        try:
            await bot.send_message(ticket["assigned_to"], f"✅ <b>Ticket {ticket_id} has been closed.</b>\nDialog ended.")
        except Exception as e:
            logger.error(f"Error notifying moderator about ticket closure: {e}")
    # Clear user_active_tickets
    if ticket["user_id"] in data_manager.data["command"]["user_active_tickets"]:
        del data_manager.data["command"]["user_active_tickets"][ticket["user_id"]]

# --- Check moderator/admin permissions ---
def is_commander(user_id: int) -> bool:
    """Check if the user is an administrator or moderator"""
    return user_id in Config.ADMIN_IDS or user_id in data_manager.data["units"][UnitType.CENTURIONS]

# --- Remove user from database when bot is blocked ---
def remove_user_from_database(user_id: int):
    """Remove user from all database structures"""
    # Remove from all units
    for unit_type in UnitType.ALL_TYPES:
        data_manager.data["units"][unit_type].discard(user_id)
    # Remove from active users
    data_manager.data["combat_ready"].discard(user_id)
    # Remove from subscribers
    data_manager.data["subscribers"].discard(user_id)
    # Remove from call signs
    data_manager.data["command"]["call_signs"].pop(user_id, None)
    # Remove from activity
    data_manager.data["command"]["activity"].pop(user_id, None)
    # Remove from temporary actions
    data_manager.data["command"]["temp_actions"].pop(user_id, None)
    # Remove from temporary missions
    data_manager.data["command"]["temp_missions"].pop(user_id, None)
    # Remove from active tickets
    data_manager.data["command"]["user_active_tickets"].pop(user_id, None)
    # Remove from usernames
    data_manager.data["usernames"].pop(str(user_id), None)
    # Remove from ticket responses
    for ticket_id in list(data_manager.data["command"]["ticket_responses"].keys()):
        data_manager.data["command"]["ticket_responses"][ticket_id].pop(user_id, None)
        if not data_manager.data["command"]["ticket_responses"][ticket_id]:
            del data_manager.data["command"]["ticket_responses"][ticket_id]
    # Remove user's tickets
    for ticket_id in list(data_manager.data["command"]["tickets"].keys()):
        ticket = data_manager.data["command"]["tickets"][ticket_id]
        if ticket.get("user_id") == user_id or ticket.get("assigned_to") == user_id:
            del data_manager.data["command"]["tickets"][ticket_id]
    # Remove from completed missions
    for mission in data_manager.data["missions"]["archive"].values():
        if "completed_by" in mission and isinstance(mission["completed_by"], set):
            mission["completed_by"].discard(user_id)
    data_manager.save_data()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("System shutdown requested by user")
    except Exception as e:
        logger.critical(f"Critical error: {e}")
        raise