{\rtf1\ansi\ansicpg1252\cocoartf2822
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww30040\viewh16580\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 // server.js - Railway WebSocket Bridge f\'fcr AWS API Gateway zu Supabase\
const WebSocket = require('ws');\
const \{ createClient \} = require('@supabase/supabase-js');\
const express = require('express');\
\
const app = express();\
const port = process.env.PORT || 3000;\
\
// Konfiguration\
const AWS_WEBSOCKET_URL = 'wss://qk3ytibzxc.execute-api.ap-southeast-1.amazonaws.com/production';\
const SUPABASE_URL = process.env.SUPABASE_URL;\
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;\
const TENANT_ID = process.env.TENANT_ID || 'default';\
\
// Supabase Client mit Service Role Key\
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, \{\
  auth: \{ persistSession: false \}\
\});\
\
// WebSocket Management\
let awsWebSocket = null;\
let sessionId = null;\
let reconnectAttempts = 0;\
const maxReconnectAttempts = 10;\
let isConnecting = false;\
\
app.use(express.json());\
\
// Health Check Endpoint\
app.get('/health', (req, res) => \{\
  const status = \{\
    service: 'AWS WebSocket Bridge',\
    websocket_status: awsWebSocket?.readyState === WebSocket.OPEN ? 'connected' : 'disconnected',\
    session_id: sessionId,\
    uptime: process.uptime(),\
    timestamp: new Date().toISOString()\
  \};\
  res.json(status);\
\});\
\
// Manual Connect Endpoint\
app.post('/connect', (req, res) => \{\
  connectToAWS();\
  res.json(\{ message: 'Connection attempt initiated' \});\
\});\
\
// Ihre MovFlee Parser Funktion\
function parseMovFleeMessage(payload, telemetry) \{\
  const result = \{\
    device_id: null,\
    vehicle_id: null,\
    location_data: null,\
    fuel_data: null,\
    charge_data: null,\
    trip_data: null,\
    engine_data: null,\
    state_data: null,\
    odometer_data: null,\
    misc_data: null\
  \};\
\
  // Extract device and vehicle IDs\
  if (telemetry.device_id) result.device_id = telemetry.device_id;\
  if (telemetry.vehicle_id || payload.vehicleId) \{\
    result.vehicle_id = telemetry.vehicle_id || payload.vehicleId;\
  \}\
\
  // Parse location data\
  if (telemetry.location?.latitude && telemetry.location?.longitude) \{\
    result.location_data = \{\
      latitude: telemetry.location.latitude,\
      longitude: telemetry.location.longitude,\
      timestamp: telemetry.location.timestamp !== "0001-01-01T00:00:00Z" \
        ? telemetry.location.timestamp \
        : telemetry.timestamp\
    \};\
\
    const rawState = telemetry.raw_data?.state?.reported;\
    if (rawState) \{\
      if (rawState.sp !== undefined) result.location_data.speed = rawState.sp;\
      if (rawState.alt !== undefined) result.location_data.altitude = rawState.alt;\
      if (rawState.ang !== undefined) result.location_data.heading = rawState.ang;\
      if (rawState.sat !== undefined) result.location_data.satellites = rawState.sat;\
    \}\
  \}\
\
  // Parse engine/vehicle data from raw_data.state.reported\
  const rawState = telemetry.raw_data?.state?.reported;\
  if (rawState) \{\
    const engineData = \{\};\
    const stateData = \{\};\
    const fuelData = \{\};\
    const miscRawData = \{\};\
\
    Object.entries(rawState).forEach(([key, value]) => \{\
      switch(key) \{\
        case '16':\
          if (!result.odometer_data) result.odometer_data = \{\};\
          result.odometer_data.total_distance = value;\
          break;\
        case '66':\
        case '67':\
          engineData[`voltage_$\{key\}`] = value;\
          break;\
        case '68':\
          engineData.battery_current = value;\
          break;\
        case '113':\
          fuelData.level_percentage = value;\
          break;\
        case '21':\
          stateData.ignition_state = value;\
          break;\
        case '239':\
        case '240':\
        case '241':\
          stateData[`state_$\{key\}`] = value;\
          break;\
        case '256':\
          if (!result.misc_data) result.misc_data = \{\};\
          result.misc_data.vin = value;\
          break;\
        case '389':\
        case '390':\
          if (!result.odometer_data) result.odometer_data = \{\};\
          if (key === '389') result.odometer_data.total_distance_alt = value;\
          if (key === '390') result.odometer_data.trip_distance = value;\
          break;\
        case 'sp':\
        case 'alt':\
        case 'ang':\
        case 'sat':\
        case 'latlng':\
        case 'ts':\
        case 'evt':\
        case 'pr':\
          break;\
        default:\
          miscRawData[key] = value;\
      \}\
    \});\
\
    if (Object.keys(engineData).length > 0) result.engine_data = engineData;\
    if (Object.keys(stateData).length > 0) result.state_data = stateData;\
    if (Object.keys(fuelData).length > 0) result.fuel_data = fuelData;\
    if (Object.keys(miscRawData).length > 0) \{\
      if (!result.misc_data) result.misc_data = \{\};\
      result.misc_data.raw_state_data = miscRawData;\
    \}\
  \}\
\
  // Add device metadata\
  if (telemetry.device_type || telemetry.device_serial_number || \
      telemetry.vehicle_make || telemetry.vehicle_model) \{\
    if (!result.misc_data) result.misc_data = \{\};\
    if (telemetry.device_type) result.misc_data.device_type = telemetry.device_type;\
    if (telemetry.device_serial_number) result.misc_data.device_serial_number = telemetry.device_serial_number;\
    if (telemetry.vehicle_make) result.misc_data.vehicle_make = telemetry.vehicle_make;\
    if (telemetry.vehicle_model) result.misc_data.vehicle_model = telemetry.vehicle_model;\
    if (telemetry.id) result.misc_data.telemetry_id = telemetry.id;\
    if (telemetry.created_at) result.misc_data.created_at = telemetry.created_at;\
  \}\
\
  return result;\
\}\
\
// Session Management\
async function createWebSocketSession() \{\
  try \{\
    const \{ data, error \} = await supabase\
      .from('telematic_websocket_sessions')\
      .insert(\{\
        tenant_id: TENANT_ID,\
        connected_at: new Date().toISOString(),\
        status: 'connected',\
        websocket_url: AWS_WEBSOCKET_URL\
      \})\
      .select('id')\
      .single();\
\
    if (error) throw error;\
    console.log(`WebSocket session created: $\{data.id\}`);\
    return data.id;\
  \} catch (error) \{\
    console.error('Failed to create WebSocket session:', error);\
    return null;\
  \}\
\}\
\
async function updateSessionStatus(status, errorMessage = null) \{\
  if (!sessionId) return;\
\
  try \{\
    const updateData = \{\
      status,\
      last_message_at: new Date().toISOString()\
    \};\
\
    if (errorMessage) updateData.error_message = errorMessage;\
\
    await supabase\
      .from('telematic_websocket_sessions')\
      .update(updateData)\
      .eq('id', sessionId);\
  \} catch (error) \{\
    console.error('Failed to update session status:', error);\
  \}\
\}\
\
// AWS WebSocket Connection\
async function connectToAWS() \{\
  if (isConnecting || (awsWebSocket && awsWebSocket.readyState === WebSocket.OPEN)) \{\
    console.log('Already connecting or connected');\
    return;\
  \}\
\
  isConnecting = true;\
  console.log(`Connecting to AWS WebSocket: $\{AWS_WEBSOCKET_URL\}`);\
\
  try \{\
    awsWebSocket = new WebSocket(AWS_WEBSOCKET_URL);\
\
    awsWebSocket.on('open', async () => \{\
      console.log('\uc0\u9989  Connected to AWS API Gateway WebSocket');\
      isConnecting = false;\
      reconnectAttempts = 0;\
      sessionId = await createWebSocketSession();\
      \
      // Optional: Send initial authentication or subscription message\
      // awsWebSocket.send(JSON.stringify(\{ action: 'subscribe', tenant: TENANT_ID \}));\
    \});\
\
    awsWebSocket.on('message', async (data) => \{\
      try \{\
        console.log(`\uc0\u55357 \u56552  Received message:`, data.toString());\
        \
        const messageData = JSON.parse(data.toString());\
        \
        // Handle MovFlee message structure\
        const payload = messageData.payload || messageData;\
        const telemetry = payload.telemetry || \{\};\
        \
        // Extract message metadata\
        const updateType = payload.update_type || messageData.type || 'telemetry_update';\
        const messageTimestamp = telemetry.timestamp || \
                                payload.timestamp || \
                                messageData.timestamp || \
                                new Date().toISOString();\
        \
        // Parse telemetry data\
        const parsedData = parseMovFleeMessage(payload, telemetry);\
        \
        // Prepare data for Supabase\
        const insertData = \{\
          tenant_id: TENANT_ID,\
          update_type: updateType,\
          timestamp: messageTimestamp,\
          raw_telemetry: messageData,\
          ...parsedData\
        \};\
\
        // Remove null/undefined fields\
        Object.keys(insertData).forEach(key => \{\
          if (insertData[key] === null || insertData[key] === undefined) \{\
            delete insertData[key];\
          \}\
        \});\
\
        // Store in Supabase\
        const \{ error: insertError \} = await supabase\
          .from('telematic_data_streams')\
          .insert(insertData);\
\
        if (insertError) \{\
          console.error('\uc0\u10060  Error storing telematic data:', insertError);\
        \} else \{\
          console.log('\uc0\u9989  Successfully stored telematic data');\
        \}\
\
        // Update session activity\
        if (sessionId) \{\
          await supabase\
            .from('telematic_websocket_sessions')\
            .update(\{\
              last_message_at: new Date().toISOString(),\
              message_count: supabase.raw('COALESCE(message_count, 0) + 1')\
            \})\
            .eq('id', sessionId);\
        \}\
\
      \} catch (error) \{\
        console.error('\uc0\u10060  Error processing message:', error);\
        await updateSessionStatus('error', error.message);\
      \}\
    \});\
\
    awsWebSocket.on('error', async (error) => \{\
      console.error('\uc0\u10060  WebSocket error:', error);\
      isConnecting = false;\
      await updateSessionStatus('error', error.message);\
    \});\
\
    awsWebSocket.on('close', async (code, reason) => \{\
      console.log(`\uc0\u55357 \u56588  WebSocket closed. Code: $\{code\}, Reason: $\{reason\}`);\
      isConnecting = false;\
      awsWebSocket = null;\
      \
      await updateSessionStatus('disconnected', `Connection closed: $\{code\} - $\{reason\}`);\
\
      // Automatic reconnection with exponential backoff\
      if (reconnectAttempts < maxReconnectAttempts) \{\
        const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000); // Max 30s\
        console.log(`\uc0\u55357 \u56580  Reconnecting in $\{delay\}ms... (Attempt $\{reconnectAttempts + 1\}/$\{maxReconnectAttempts\})`);\
        \
        setTimeout(() => \{\
          reconnectAttempts++;\
          connectToAWS();\
        \}, delay);\
      \} else \{\
        console.error('\uc0\u10060  Max reconnection attempts reached');\
        await updateSessionStatus('failed', 'Max reconnection attempts exceeded');\
      \}\
    \});\
\
  \} catch (error) \{\
    console.error('\uc0\u10060  Failed to create WebSocket connection:', error);\
    isConnecting = false;\
    await updateSessionStatus('error', error.message);\
  \}\
\}\
\
// Graceful shutdown\
process.on('SIGINT', async () => \{\
  console.log('\uc0\u55357 \u57041  Shutting down gracefully...');\
  \
  if (awsWebSocket) \{\
    awsWebSocket.close();\
  \}\
  \
  if (sessionId) \{\
    await updateSessionStatus('disconnected', 'Service shutdown');\
  \}\
  \
  process.exit(0);\
\});\
\
// Start server and connect to AWS\
app.listen(port, () => \{\
  console.log(`\uc0\u55357 \u56960  WebSocket Bridge Server running on port $\{port\}`);\
  console.log(`\uc0\u55357 \u56599  AWS WebSocket URL: $\{AWS_WEBSOCKET_URL\}`);\
  console.log(`\uc0\u55357 \u56522  Supabase URL: $\{SUPABASE_URL\}`);\
  \
  // Initial connection to AWS WebSocket\
  setTimeout(() => \{\
    connectToAWS();\
  \}, 1000);\
\});\
\
// Keep alive ping (optional, for AWS API Gateway)\
setInterval(() => \{\
  if (awsWebSocket && awsWebSocket.readyState === WebSocket.OPEN) \{\
    awsWebSocket.ping();\
  \}\
\}, 30000); // Every 30 seconds}