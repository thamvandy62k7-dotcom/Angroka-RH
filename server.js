require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const mongoose = require('mongoose');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

// ១. ភ្ជាប់ទៅកាន់ MongoDB
const MONGODB_URI = process.env.MONGODB_URI;
mongoose.connect(MONGODB_URI)
  .then(() => console.log('✅ ភ្ជាប់ទៅកាន់ MongoDB ជោគជ័យ!'))
  .catch(err => console.error('❌ បរាជ័យក្នុងការភ្ជាប់ MongoDB:', err));

// ២. បង្កើត Schema សម្រាប់ទិន្នន័យ Queue
const queueSchema = new mongoose.Schema({
  id: { type: String, default: 'main_queue_system' },
  lastDate: String,
  departmentCounters: { type: mongoose.Schema.Types.Mixed, default: {} },
  waitingLists: { type: mongoose.Schema.Types.Mixed, default: {} },
  roomStates: { type: mongoose.Schema.Types.Mixed, default: {} }
}, { minimize: false });

const QueueModel = mongoose.model('QueueData', queueSchema);

// 🌟 បង្កើត Function ចាប់យកថ្ងៃខែឆ្នាំ តាមម៉ោងនៅប្រទេសកម្ពុជា
function getCurrentDateKH() {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Phnom_Penh' });
}

// ៣. តម្លៃដើម (Default Values)
let roomStates = {
  OPD: { 1: { currentTicket: '---', status: 'OFFLINE' }, 2: { currentTicket: '---', status: 'OFFLINE' }, 3: { currentTicket: '---', status: 'OFFLINE' }, 4: { currentTicket: '---', status: 'OFFLINE' } },
  NCD: { 1: { currentTicket: '---', status: 'OFFLINE' } },
  OBGYN: { 1: { currentTicket: '---', status: 'OFFLINE' } },
  ARV: { 1: { currentTicket: '---', status: 'OFFLINE' } }
};

let waitingLists = { OPD: [], NCD: [], OBGYN: [], ARV: [] };
let departmentCounters = { OPD: 0, NCD: 0, OBGYN: 0, ARV: 0 };
let lastDate = getCurrentDateKH(); // ប្រើប្រាស់ម៉ោងកម្ពុជា
const departmentPrefixes = { OPD: 'A', NCD: 'B', OBGYN: 'C', ARV: 'D' };

// ៤. ទាញយកទិន្នន័យពី MongoDB ពេល Server ដំណើរការដំបូង
async function loadStoredData() {
  try {
    const data = await QueueModel.findOne({ id: 'main_queue_system' });
    const currentDate = getCurrentDateKH();

    if (data) {
      if (data.lastDate === currentDate) {
        lastDate = data.lastDate;
        if (data.departmentCounters) departmentCounters = data.departmentCounters;
        if (data.waitingLists) waitingLists = data.waitingLists;
        if (data.roomStates) roomStates = data.roomStates;
        console.log('✅ ទាញយកទិន្នន័យលេខរៀងចាស់បានជោគជ័យ!');
      } else {
        // ឆ្លងចូលថ្ងៃថ្មី ធ្វើការ Reset
        console.log(`[DATE CHANGED] កំណត់លេខរៀងសារថ្មីសម្រាប់ថ្ងៃ: ${currentDate}`);
        lastDate = currentDate;
        departmentCounters = { OPD: 0, NCD: 0, OBGYN: 0, ARV: 0 };
        waitingLists = { OPD: [], NCD: [], OBGYN: [], ARV: [] };
        Object.keys(roomStates).forEach(dept => {
          Object.keys(roomStates[dept]).forEach(room => {
            roomStates[dept][room].currentTicket = '---';
          });
        });
        await saveStoredData(); 
      }
    } else {
      await saveStoredData(); // បង្កើតទិន្នន័យថ្មីប្រសិនបើមិនទាន់មាន
    }
  } catch (err) {
    console.error('Error loading from MongoDB:', err);
  }
}

// ៥. រក្សាទុកទិន្នន័យទៅ MongoDB វិញ
async function saveStoredData() {
  try {
    await QueueModel.findOneAndUpdate(
      { id: 'main_queue_system' },
      { lastDate, departmentCounters, waitingLists, roomStates },
      { upsert: true, new: true }
    );
  } catch (err) {
    console.error('Error saving to MongoDB:', err);
  }
}

// ៦. ពិនិត្យពេលឆ្លងថ្ងៃ
async function checkAndResetDailyQueue() {
  const currentDate = getCurrentDateKH();
  if (lastDate !== currentDate) {
    console.log(`[DATE CHANGED] Resetting queue counter for new day: ${currentDate}`);
    departmentCounters = { OPD: 0, NCD: 0, OBGYN: 0, ARV: 0 };
    waitingLists = { OPD: [], NCD: [], OBGYN: [], ARV: [] };
    
    Object.keys(roomStates).forEach(dept => {
      Object.keys(roomStates[dept]).forEach(room => {
        roomStates[dept][room].currentTicket = '---';
      });
    });

    lastDate = currentDate;
    await saveStoredData(); 
    io.emit('update-rooms', roomStates);
    io.emit('update-waiting', waitingLists);
  }
}

// Initialize Database
loadStoredData();

// ៧. Socket.io Events
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);
  
  // ពេលភ្ញៀវភ្ជាប់មក ហៅប្រាប់គាត់ពីទិន្នន័យបច្ចុប្បន្ន
  socket.emit('update-rooms', roomStates);
  socket.emit('update-waiting', waitingLists);

  socket.on('request-ticket', async (data) => {
    await checkAndResetDailyQueue(); 
    let dept = (data && data.dept) ? String(data.dept).trim().toUpperCase() : 'OPD';
    if (!departmentCounters.hasOwnProperty(dept)) dept = 'OPD';

    departmentCounters[dept] = (departmentCounters[dept] || 0) + 1;
    const prefix = departmentPrefixes[dept] || 'A';
    const ticketNum = prefix + String(departmentCounters[dept]).padStart(2, '0');

    if (!waitingLists[dept]) waitingLists[dept] = [];
    waitingLists[dept].push(ticketNum);

    // ⚡️ ១. បញ្ជូនលេខរៀងទៅម៉ាស៊ីនព្រីន និង អេក្រង់ភ្លាមៗ (ដើម្បីលឿន)
    socket.emit('ticket-generated', { dept: dept, ticket: ticketNum });
    io.emit('update-waiting', waitingLists);

    // 💾 ២. Save ចូល Database តាមក្រោយ (Background Saving) មិនចាំបាច់មាន await ទេ
    saveStoredData(); 
  });

  socket.on('get-doctor-init', async (data) => {
    await checkAndResetDailyQueue();
    socket.emit('update-rooms', roomStates);
    socket.emit('update-waiting', waitingLists);
  });

  socket.on('toggle-room-status', async (data) => {
    const dept = (data && data.dept) ? String(data.dept).trim().toUpperCase() : 'OPD';
    const room = parseInt(data.room) || 1;

    if (!roomStates[dept]) roomStates[dept] = {};
    if (!roomStates[dept][room]) roomStates[dept][room] = { currentTicket: '---', status: 'OFFLINE' };

    let currentStatus = roomStates[dept][room].status;
    let newStatus = data.status || ((currentStatus === 'ONLINE' || currentStatus === 'ACTIVE') ? 'OFFLINE' : 'ONLINE');
    roomStates[dept][room].status = newStatus;

    // ⚡️ បញ្ជូនទៅអេក្រង់ភ្លាមៗ
    io.emit('update-rooms', roomStates);
    io.emit('room-status-changed', { dept, room, status: newStatus, isOnline: newStatus === 'ONLINE', currentTicket: roomStates[dept][room].currentTicket });
    
    // 💾 Save តាមក្រោយ
    saveStoredData(); 
  });

  socket.on('call-next', async (data) => {
    await checkAndResetDailyQueue();
    const dept = (data && data.dept) ? String(data.dept).trim().toUpperCase() : 'OPD';
    const room = parseInt(data.room) || 1;

    if (waitingLists[dept] && waitingLists[dept].length > 0) {
      const nextTicket = waitingLists[dept].shift();
      if (!roomStates[dept]) roomStates[dept] = {};
      if (!roomStates[dept][room]) roomStates[dept][room] = { status: 'ONLINE' };
      
      roomStates[dept][room].currentTicket = nextTicket;

      // ⚡️ បញ្ជូនសម្លេងហៅ និងលោតលេខលើអេក្រង់ភ្លាមៗ
      io.emit('update-rooms', roomStates);
      io.emit('update-waiting', waitingLists);
      io.emit('ticket-called', { dept: dept, room: room, ticket: nextTicket, currentTicket: nextTicket });
      io.emit('play-audio-call', { dept: dept, room: room, ticket: nextTicket });

      // 💾 Save តាមក្រោយ
      saveStoredData(); 
    }
  });

  socket.on('recall-ticket', (data) => {
    const dept = (data && data.dept) ? String(data.dept).trim().toUpperCase() : 'OPD';
    const room = parseInt(data.room) || 1;
    const currentNum = roomStates[dept] && roomStates[dept][room] ? roomStates[dept][room].currentTicket : '---';

    if (currentNum && currentNum !== '---') {
      io.emit('ticket-called', { dept: dept, room: room, ticket: currentNum, currentTicket: currentNum });
      io.emit('play-audio-call', { dept: dept, room: room, ticket: currentNum });
    }
  });

  socket.on('complete-ticket', async (data) => {
    const dept = (data && data.dept) ? String(data.dept).trim().toUpperCase() : 'OPD';
    const room = parseInt(data.room) || 1;

    if (roomStates[dept] && roomStates[dept][room]) {
      roomStates[dept][room].currentTicket = '---';
    }
    
    // ⚡️ បញ្ជូនទៅអេក្រង់ភ្លាមៗ
    io.emit('update-rooms', roomStates);
    
    // 💾 Save តាមក្រោយ
    saveStoredData(); 
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Server is running on port ${PORT}`);
});
