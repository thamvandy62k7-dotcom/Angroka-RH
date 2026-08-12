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

// ៣. តម្លៃដើម (Default Values)
let roomStates = {
  OPD: { 1: { currentTicket: '---', status: 'OFFLINE' }, 2: { currentTicket: '---', status: 'OFFLINE' }, 3: { currentTicket: '---', status: 'OFFLINE' }, 4: { currentTicket: '---', status: 'OFFLINE' } },
  NCD: { 1: { currentTicket: '---', status: 'OFFLINE' } },
  OBGYN: { 1: { currentTicket: '---', status: 'OFFLINE' } },
  ARV: { 1: { currentTicket: '---', status: 'OFFLINE' } }
};

let waitingLists = { OPD: [], NCD: [], OBGYN: [], ARV: [] };
let departmentCounters = { OPD: 0, NCD: 0, OBGYN: 0, ARV: 0 };
let lastDate = new Date().toISOString().slice(0, 10);
const departmentPrefixes = { OPD: 'A', NCD: 'B', OBGYN: 'C', ARV: 'D' };

// ៤. ទាញយកទិន្នន័យពី MongoDB ពេល Server ដំណើរការដំបូង
async function loadStoredData() {
  try {
    const data = await QueueModel.findOne({ id: 'main_queue_system' });
    const currentDate = new Date().toISOString().slice(0, 10);

    if (data) {
      if (data.lastDate === currentDate) {
        lastDate = data.lastDate;
        if (data.departmentCounters) departmentCounters = data.departmentCounters;
        if (data.waitingLists) waitingLists = data.waitingLists;
        if (data.roomStates) roomStates = data.roomStates;
      } else {
        // ឆ្លងចូលថ្ងៃថ្មី ធ្វើការ Reset
        lastDate = currentDate;
        departmentCounters = { OPD: 0, NCD: 0, OBGYN: 0, ARV: 0 };
        waitingLists = { OPD: [], NCD: [], OBGYN: [], ARV: [] };
        Object.keys(roomStates).forEach(dept => {
          Object.keys(roomStates[dept]).forEach(room => {
            roomStates[dept][room].currentTicket = '---';
          });
        });
        saveStoredData();
      }
    } else {
      saveStoredData(); // បង្កើតទិន្នន័យថ្មីប្រសិនបើមិនទាន់មាន
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
function checkAndResetDailyQueue() {
  const currentDate = new Date().toISOString().slice(0, 10);
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
    saveStoredData();
    io.emit('update-rooms', roomStates);
    io.emit('update-waiting', waitingLists);
  }
}

// Initialize Database
loadStoredData();

// ៧. Socket.io Events (កូដចាស់របស់អ្នកដដែល គ្រាន់តែវាហៅ saveStoredData ទៅ Mongo)
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);
  checkAndResetDailyQueue();
  socket.emit('update-rooms', roomStates);
  socket.emit('update-waiting', waitingLists);

  socket.on('request-ticket', (data) => {
    checkAndResetDailyQueue();
    let dept = (data && data.dept) ? String(data.dept).trim().toUpperCase() : 'OPD';
    if (!departmentCounters.hasOwnProperty(dept)) dept = 'OPD';

    departmentCounters[dept] = (departmentCounters[dept] || 0) + 1;
    const prefix = departmentPrefixes[dept] || 'A';
    const ticketNum = prefix + String(departmentCounters[dept]).padStart(2, '0');

    if (!waitingLists[dept]) waitingLists[dept] = [];
    waitingLists[dept].push(ticketNum);

    saveStoredData();

    socket.emit('ticket-generated', { dept: dept, ticket: ticketNum });
    io.emit('update-waiting', waitingLists);
  });

  socket.on('get-doctor-init', (data) => {
    checkAndResetDailyQueue();
    socket.emit('update-rooms', roomStates);
    socket.emit('update-waiting', waitingLists);
  });

  socket.on('toggle-room-status', (data) => {
    const dept = (data && data.dept) ? String(data.dept).trim().toUpperCase() : 'OPD';
    const room = parseInt(data.room) || 1;

    if (!roomStates[dept]) roomStates[dept] = {};
    if (!roomStates[dept][room]) roomStates[dept][room] = { currentTicket: '---', status: 'OFFLINE' };

    let currentStatus = roomStates[dept][room].status;
    let newStatus = data.status || ((currentStatus === 'ONLINE' || currentStatus === 'ACTIVE') ? 'OFFLINE' : 'ONLINE');
    roomStates[dept][room].status = newStatus;

    saveStoredData(); // រក្សាទុកស្ថានភាពបន្ទប់
    io.emit('update-rooms', roomStates);
    io.emit('room-status-changed', { dept, room, status: newStatus, isOnline: newStatus === 'ONLINE', currentTicket: roomStates[dept][room].currentTicket });
  });

  socket.on('call-next', (data) => {
    checkAndResetDailyQueue();
    const dept = (data && data.dept) ? String(data.dept).trim().toUpperCase() : 'OPD';
    const room = parseInt(data.room) || 1;

    if (waitingLists[dept] && waitingLists[dept].length > 0) {
      const nextTicket = waitingLists[dept].shift();
      if (!roomStates[dept]) roomStates[dept] = {};
      if (!roomStates[dept][room]) roomStates[dept][room] = { status: 'ONLINE' };
      
      roomStates[dept][room].currentTicket = nextTicket;

      saveStoredData();
      io.emit('update-rooms', roomStates);
      io.emit('update-waiting', waitingLists);
      io.emit('ticket-called', { dept: dept, room: room, ticket: nextTicket, currentTicket: nextTicket });
      io.emit('play-audio-call', { dept: dept, room: room, ticket: nextTicket });
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

  socket.on('complete-ticket', (data) => {
    const dept = (data && data.dept) ? String(data.dept).trim().toUpperCase() : 'OPD';
    const room = parseInt(data.room) || 1;

    if (roomStates[dept] && roomStates[dept][room]) {
      roomStates[dept][room].currentTicket = '---';
    }
    saveStoredData();
    io.emit('update-rooms', roomStates);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Server is running on port ${PORT}`);
});