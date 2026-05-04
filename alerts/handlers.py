"""Telegram handlers for alert subscription management.

Uses context.user_data for state instead of ConversationHandler to avoid
PTB 21 per_message conflicts with mixed inline/text flows.

Commands:
  /subscribe   — start subscription flow
  /myalerts    — list active subscriptions
  /unsubscribe — cancel all alerts
  /deletedata  — wipe all stored data
"""

import asyncio
import json
import logging
import os

import requests
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationHandlerStop, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from alerts import db

logger = logging.getLogger(__name__)

# user_data keys
_STATE = "alert_state"
_TYPE  = "alert_type"
_LAT   = "alert_lat"
_LON   = "alert_lon"

# States
_AWAITING_ADDRESS = "awaiting_address"
_AWAITING_RADIUS  = "awaiting_radius"

ALERT_TYPES = {
    "nearby_311":      "📍 Nearby 311 Reports",
    "animal_nearby":   "🐾 Animal Incidents Near Me",
    "crime_daily":     "🚨 Daily Crime Report",
    "district_digest": "📊 Weekly District Digest",
}

DISTRICT_LABELS = {
    "1":  "District 1 (NE Austin)",
    "2":  "District 2 (SE Austin)",
    "3":  "District 3 (E Austin / Cesar Chavez)",
    "4":  "District 4 (S Austin / Oltorf)",
    "5":  "District 5 (S Austin / Slaughter)",
    "6":  "District 6 (NW / Jollyville)",
    "7":  "District 7 (N Central / Crestview)",
    "8":  "District 8 (SW / Oak Hill)",
    "9":  "District 9 (Central / Downtown)",
    "10": "District 10 (W Austin / Westlake Hills)",
}

RADIUS_OPTIONS = {
    "025": (0.25, "🏠 0.25 mi — My block"),
    "050": (0.5,  "🏘️ 0.5 mi — My neighborhood"),
    "100": (1.0,  "🗺️ 1 mi — Broader area"),
}


# ── geocoding ──────────────────────────────────────────────────────────────────

def _geocode(address: str) -> tuple[float, float] | None:
    """Geocode an address → (lat, lon) or None."""
    maps_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not maps_key:
        return None
    try:
        geo = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": f"{address}, Austin TX", "key": maps_key},
            timeout=10,
        ).json()
        if geo.get("status") != "OK":
            return None
        loc = geo["results"][0]["geometry"]["location"]
        return loc["lat"], loc["lng"]
    except Exception as e:
        logger.error(f"geocode: {e}")
        return None


def _latlon_to_district(lat: float, lon: float) -> str | None:
    """Look up Austin council district for a lat/lon via ArcGIS."""
    try:
        arcgis = requests.get(
            "https://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services"
            "/Council_Districts/FeatureServer/0/query",
            params={
                "geometry":     f"{lon},{lat}",
                "geometryType": "esriGeometryPoint",
                "inSR":         "4326",
                "spatialRel":   "esriSpatialRelIntersects",
                "outFields":    "COUNCIL_DI",
                "f":            "json",
            },
            timeout=10,
        ).json()
        features = arcgis.get("features", [])
        if not features:
            logger.warning(f"ArcGIS returned no district for {lat},{lon}")
            return None
        return str(features[0]["attributes"]["COUNCIL_DI"])
    except Exception as e:
        logger.error(f"arcgis district lookup: {e}")
        return None


# ── keyboards ──────────────────────────────────────────────────────────────────

def _type_picker() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📍 Nearby 311 Reports",      callback_data="sub_type_nearby_311")],
        [InlineKeyboardButton("🐾 Animal Incidents Near Me",callback_data="sub_type_animal_nearby")],
        [InlineKeyboardButton("❌ Cancel",                   callback_data="sub_cancel")],
    ])


def _district_picker() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for d in [str(i) for i in range(1, 11)]:
        row.append(InlineKeyboardButton(f"D{d}", callback_data=f"sub_district_{d}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("📍 Enter my address instead", callback_data="sub_enter_address")])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="sub_cancel")])
    return InlineKeyboardMarkup(rows)


def _radius_picker() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(label, callback_data=f"sub_radius_{key}")]
        for key, (_, label) in RADIUS_OPTIONS.items()
    ] + [[InlineKeyboardButton("❌ Cancel", callback_data="sub_cancel")]])


# ── /subscribe ─────────────────────────────────────────────────────────────────

async def subscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.clear()
    await update.message.reply_text(
        "📬 *Choose an alert type:*",
        parse_mode="Markdown",
        reply_markup=_type_picker(),
    )


async def subscribe_button_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    context.user_data.clear()
    await query.message.reply_text(
        "📬 *Choose an alert type:*",
        parse_mode="Markdown",
        reply_markup=_type_picker(),
    )


async def choose_type_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    alert_type = query.data.replace("sub_type_", "")
    context.user_data[_TYPE] = alert_type

    _location_based = {"nearby_311", "animal_nearby"}
    _intros = {
        "nearby_311":    ("📍 *Nearby 311 Reports*",      "New 311 requests filed within your chosen radius, sent each morning."),
        "animal_nearby": ("🐾 *Animal Incidents Near Me*", "Loose dogs, bites, and wildlife reports within your chosen radius, sent each morning."),
    }
    if alert_type in _location_based:
        context.user_data[_STATE] = _AWAITING_ADDRESS
        title, desc = _intros[alert_type]
        await query.edit_message_text(
            f"{title}\n_{desc}_\n\nType your Austin street address or intersection:",
            parse_mode="Markdown",
        )
        return

    # Crime alerts — show district picker
    descs = {
        "crime_daily":     "New incidents in your district, sent each morning",
        "district_digest": "Week-over-week crime summary, sent every Monday",
    }
    label = ALERT_TYPES[alert_type]
    await query.edit_message_text(
        f"*{label}*\n_{descs.get(alert_type, '')}_\n\nPick your council district:",
        parse_mode="Markdown",
        reply_markup=_district_picker(),
    )


async def choose_district_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    district   = query.data.replace("sub_district_", "")
    alert_type = context.user_data.get(_TYPE, "crime_daily")
    user       = update.effective_user

    db.upsert_user(user.id, update.effective_chat.id)
    db.add_subscription(user.id, alert_type, district=district)
    context.user_data.clear()

    type_label = ALERT_TYPES.get(alert_type, "Alert")
    schedule   = "each morning" if "daily" in alert_type else "every Monday"
    dist_label = DISTRICT_LABELS.get(district, f"District {district}")
    await query.edit_message_text(
        f"✅ *Subscribed!*\n\n*Alert:* {type_label}\n*District:* {dist_label}\n*Schedule:* {schedule}\n\n"
        f"Use /myalerts to manage or /unsubscribe to stop.",
        parse_mode="Markdown",
    )


async def choose_radius_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    key = query.data.replace("sub_radius_", "")
    radius, radius_label = RADIUS_OPTIONS.get(key, (0.5, "0.5 mi"))

    lat = context.user_data.get(_LAT)
    lon = context.user_data.get(_LON)
    alert_type = context.user_data.get(_TYPE)

    if not lat or not lon:
        await query.edit_message_text("Something went wrong — please try /subscribe again.")
        context.user_data.clear()
        return

    user = update.effective_user
    params = json.dumps({"lat": lat, "lon": lon, "radius_miles": radius})
    db.upsert_user(user.id, update.effective_chat.id)
    db.add_subscription(user.id, alert_type, params=params)
    context.user_data.clear()

    type_label = ALERT_TYPES.get(alert_type, "Alert")
    await query.edit_message_text(
        f"✅ *Subscribed!*\n\n"
        f"*Alert:* {type_label}\n"
        f"*Radius:* {radius_label}\n"
        f"*Schedule:* Each morning\n\n"
        f"Use /myalerts to manage or /unsubscribe to stop.",
        parse_mode="Markdown",
    )


# ── deep-link entry from austin311.com map popups ─────────────────────────────

# Map short codes used in t.me/austin311bot?start=sub_<code>_<lat>_<lon> payloads
# to the full alert_type values stored in the DB.
_DEEP_LINK_TYPES = {
    "311":    "nearby_311",
    "animal": "animal_nearby",
}


async def start_subscribe_with_location(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    payload: str,
) -> bool:
    """Resume the subscribe flow at the radius-picker step.

    Called from /start when the user arrived via a deep link of the form
    `sub_<type>_<lat_microdeg>_<lon_microdeg>`. Returns True if the payload
    was understood and a reply was sent; False otherwise.
    """
    parts = payload.split("_")
    if len(parts) != 4 or parts[0] != "sub":
        return False
    alert_type = _DEEP_LINK_TYPES.get(parts[1])
    if not alert_type:
        return False
    try:
        lat = int(parts[2]) / 1_000_000
        lon = int(parts[3]) / 1_000_000
    except ValueError:
        return False
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return False

    context.user_data.clear()
    context.user_data[_TYPE] = alert_type
    context.user_data[_LAT] = lat
    context.user_data[_LON] = lon

    type_label = ALERT_TYPES.get(alert_type, "Alert")
    await update.message.reply_text(
        f"📍 *{type_label}*\n_Location pinned from austin311.com_\n\n"
        "How far out should we watch?",
        parse_mode="Markdown",
        reply_markup=_radius_picker(),
    )
    return True


async def enter_address_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    context.user_data[_STATE] = _AWAITING_ADDRESS
    await query.edit_message_text(
        "📍 Type your Austin street address, neighborhood, or zip code:",
    )


async def cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    context.user_data.clear()
    await query.edit_message_text("Cancelled.")


# ── text message handler ───────────────────────────────────────────────────────

async def receive_address(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.user_data.get(_STATE) != _AWAITING_ADDRESS:
        return  # not our message — let echo handler take it

    address = update.message.text.strip()
    context.user_data.pop(_STATE, None)
    alert_type = context.user_data.get(_TYPE, "")

    msg = await update.message.reply_text("⏳ Looking up your location...")
    coords = await asyncio.to_thread(_geocode, address)

    if not coords:
        await msg.edit_text(
            "❌ Couldn't geocode that address. Try a street name or intersection."
        )
        context.user_data.clear()
        raise ApplicationHandlerStop

    lat, lon = coords

    if alert_type in {"nearby_311", "animal_nearby"}:
        context.user_data[_LAT] = lat
        context.user_data[_LON] = lon
        await msg.edit_text(
            "📍 *Got your location.* How far out should we watch?",
            parse_mode="Markdown",
            reply_markup=_radius_picker(),
        )
        raise ApplicationHandlerStop

    # Crime alerts — look up district
    district = await asyncio.to_thread(_latlon_to_district, lat, lon)
    if not district:
        await msg.edit_text(
            "📍 We couldn't pin your exact district — zip codes often cross district lines.\n\n"
            "Find yours here: [Austin Council District Map](https://www.austintexas.gov/GIS/CouncilDistrictMap)\n\n"
            "Then pick below:",
            parse_mode="Markdown",
            reply_markup=_district_picker(),
            disable_web_page_preview=True,
        )
        raise ApplicationHandlerStop

    await msg.delete()
    user = update.effective_user
    db.upsert_user(user.id, update.effective_chat.id)
    db.add_subscription(user.id, alert_type, district=district)
    context.user_data.clear()

    dist_label = DISTRICT_LABELS.get(district, f"District {district}")
    type_label = ALERT_TYPES.get(alert_type, "Alert")
    schedule   = "each morning" if "daily" in alert_type else "every Monday"
    await update.message.reply_text(
        f"✅ *Subscribed!*\n\n*Alert:* {type_label}\n*District:* {dist_label}\n*Schedule:* {schedule}\n\n"
        f"Use /myalerts to manage or /unsubscribe to stop.",
        parse_mode="Markdown",
    )
    raise ApplicationHandlerStop


# ── /myalerts ──────────────────────────────────────────────────────────────────

async def myalerts_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    subs = db.get_user_subscriptions(update.effective_user.id)
    if not subs:
        await update.message.reply_text("You have no active alerts. Use /subscribe to set one up.")
        return

    keyboard, lines = [], []
    for sub in subs:
        type_label = ALERT_TYPES.get(sub["alert_type"], sub["alert_type"])
        if sub["district"]:
            loc = DISTRICT_LABELS.get(sub["district"], f"District {sub['district']}")
        elif sub["params"]:
            try:
                p = json.loads(sub["params"])
                loc = f"{p.get('radius_miles', 0.5)} mi radius"
            except Exception:
                loc = "custom location"
        else:
            loc = "unknown"
        short = f"{type_label} — {loc}"
        lines.append(f"• {short}")
        keyboard.append([InlineKeyboardButton(f"❌ Cancel: {short[:45]}", callback_data=f"unsub_{sub['id']}")])

    await update.message.reply_text(
        "📬 *Your active alerts:*\n" + "\n".join(lines),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def cancel_subscription_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    sub_id = int(query.data.replace("unsub_", ""))
    removed = db.deactivate_subscription(sub_id, update.effective_user.id)
    await query.edit_message_text("✅ Alert cancelled." if removed else "Alert not found.")


# ── /unsubscribe ───────────────────────────────────────────────────────────────

async def unsubscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    count = db.deactivate_all(update.effective_user.id)
    await update.message.reply_text(
        f"✅ Cancelled {count} alert{'s' if count != 1 else ''}."
        if count else "You have no active alerts."
    )


# ── /deletedata ────────────────────────────────────────────────────────────────

async def deletedata_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db.delete_user_data(update.effective_user.id)
    await update.message.reply_text(
        "🗑️ Done. All your alert preferences and stored data have been deleted."
    )


# ── inline versions (called from alerts_menu inline button) ───────────────────

async def myalerts_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    subs = db.get_user_subscriptions(update.effective_user.id)
    if not subs:
        await query.edit_message_text(
            "You have no active alerts. Use /subscribe to set one up."
        )
        return
    keyboard, lines = [], []
    for sub in subs:
        type_label = ALERT_TYPES.get(sub["alert_type"], sub["alert_type"])
        if sub["district"]:
            loc = DISTRICT_LABELS.get(sub["district"], f"District {sub['district']}")
        elif sub["params"]:
            try:
                p = json.loads(sub["params"])
                loc = f"{p.get('radius_miles', 0.5)} mi radius"
            except Exception:
                loc = "custom location"
        else:
            loc = "unknown"
        short = f"{type_label} — {loc}"
        lines.append(f"• {short}")
        keyboard.append([InlineKeyboardButton(f"❌ Cancel: {short[:45]}", callback_data=f"unsub_{sub['id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="alerts_menu")])
    await query.edit_message_text(
        "📬 *Your active alerts:*\n" + "\n".join(lines),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def unsubscribe_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    count = db.deactivate_all(update.effective_user.id)
    await query.edit_message_text(
        f"✅ Cancelled {count} alert{'s' if count != 1 else ''}."
        if count else "You have no active alerts."
    )


async def deletedata_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    db.delete_user_data(update.effective_user.id)
    await query.edit_message_text(
        "🗑️ Done. All your alert preferences and stored data have been deleted."
    )


# ── handler registration ───────────────────────────────────────────────────────

def register_alert_handlers(app) -> None:
    app.add_handler(CommandHandler("subscribe",   subscribe_command))
    app.add_handler(CommandHandler("myalerts",    myalerts_command))
    app.add_handler(CommandHandler("unsubscribe", unsubscribe_command))
    app.add_handler(CommandHandler("deletedata",  deletedata_command))

    app.add_handler(CallbackQueryHandler(subscribe_button_entry,       pattern=r"^subscribe_start$"))
    app.add_handler(CallbackQueryHandler(myalerts_cb,                  pattern=r"^alerts_myalerts$"))
    app.add_handler(CallbackQueryHandler(unsubscribe_cb,               pattern=r"^alerts_unsubscribe$"))
    app.add_handler(CallbackQueryHandler(deletedata_cb,                pattern=r"^alerts_deletedata$"))
    app.add_handler(CallbackQueryHandler(choose_type_callback,         pattern=r"^sub_type_"))
    app.add_handler(CallbackQueryHandler(choose_district_callback,     pattern=r"^sub_district_"))
    app.add_handler(CallbackQueryHandler(choose_radius_callback,       pattern=r"^sub_radius_"))
    app.add_handler(CallbackQueryHandler(enter_address_callback,       pattern=r"^sub_enter_address$"))
    app.add_handler(CallbackQueryHandler(cancel_callback,              pattern=r"^sub_cancel$"))
    app.add_handler(CallbackQueryHandler(cancel_subscription_callback, pattern=r"^unsub_\d+$"))

    # Runs before echo handler; no-ops when user isn't in address flow
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, receive_address), group=-1)
