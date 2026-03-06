import { useState, useEffect, useRef, useCallback, useContext } from 'react';
import { AuthContext } from './AuthContext';
import { API_BASE_URL } from './config';
import Editor from '@monaco-editor/react';
import axios from 'axios';
import {
    Copy, Check, Users, ArrowLeft, Play, Loader2, Send, Wifi, WifiOff,
    Swords, Crown, UserPlus, LogOut, Tags, X, MessageCircle, Mic, MicOff, Phone, PhoneOff
} from 'lucide-react';
import './TwinChallenge.css';

// WebSocket URL from API_BASE_URL (convert http→ws, https→wss)
const WS_BASE_URL = API_BASE_URL.replace(/^http/, 'ws');

// Same topic lists as the main sandbox
const TOPICS = [
    { id: 'select_filter', label: 'Select & Filter', category: 'Foundational' },
    { id: 'sorting', label: 'Sorting & Ordering', category: 'Foundational' },
    { id: 'nulls', label: 'Handling Nulls', category: 'Foundational' },
    { id: 'casting', label: 'Type Casting & Schema', category: 'Foundational' },
    { id: 'duplicates', label: 'Remove Duplicates', category: 'Foundational' },
    { id: 'math', label: 'Basic Math Operations', category: 'Foundational' },
    { id: 'agg', label: 'Aggregations & GroupBy', category: 'Intermediate' },
    { id: 'join', label: 'Joins', category: 'Intermediate' },
    { id: 'date', label: 'Date & Time Operations', category: 'Intermediate' },
    { id: 'string', label: 'String Manipulations', category: 'Intermediate' },
    { id: 'pivot', label: 'Pivot & Unpivot', category: 'Intermediate' },
    { id: 'logic', label: 'Conditional Logic (when/otherwise)', category: 'Intermediate' },
    { id: 'unions', label: 'Unions', category: 'Intermediate' },
    { id: 'window', label: 'Window Functions', category: 'Advanced' },
    { id: 'complex', label: 'Array & Map Operations', category: 'Advanced' },
    { id: 'json', label: 'JSON Parsing', category: 'Advanced' },
    { id: 'udf', label: 'UDFs (User Defined Functions)', category: 'Advanced' },
    { id: 'optimiz', label: 'Performance & Optimization', category: 'Advanced' },
    { id: 'hof', label: 'High-Order Functions', category: 'Advanced' },
];

const SQL_TOPICS = [
    { id: 'sql_select', label: 'SELECT & WHERE', category: 'Basic' },
    { id: 'sql_order', label: 'ORDER BY & LIMIT', category: 'Basic' },
    { id: 'sql_distinct', label: 'DISTINCT & Aliases', category: 'Basic' },
    { id: 'sql_nulls', label: 'NULL Handling', category: 'Basic' },
    { id: 'sql_inner_join', label: 'INNER JOIN', category: 'Intermediate' },
    { id: 'sql_outer_join', label: 'LEFT / RIGHT JOIN', category: 'Intermediate' },
    { id: 'sql_groupby', label: 'GROUP BY & HAVING', category: 'Intermediate' },
    { id: 'sql_agg', label: 'Aggregations (COUNT, SUM, AVG)', category: 'Intermediate' },
    { id: 'sql_case', label: 'CASE WHEN', category: 'Intermediate' },
    { id: 'sql_subquery', label: 'Subqueries', category: 'Advanced' },
    { id: 'sql_cte', label: 'CTEs (WITH clause)', category: 'Advanced' },
    { id: 'sql_window', label: 'Window Functions', category: 'Advanced' },
    { id: 'sql_set', label: 'UNION & Set Operations', category: 'Advanced' },
    { id: 'sql_self_join', label: 'Self Joins', category: 'Advanced' },
];

function TwinChallenge({ onBack }) {
    const { user, token } = useContext(AuthContext);

    // --- Room State ---
    const [phase, setPhase] = useState('lobby'); // lobby | waiting | challenge
    const [roomCode, setRoomCode] = useState('');
    const [joinInput, setJoinInput] = useState('');
    const [roomInfo, setRoomInfo] = useState(null);
    const [error, setError] = useState('');
    const [copied, setCopied] = useState(false);
    const [roomClosedMsg, setRoomClosedMsg] = useState('');

    // --- Challenge State ---
    const [problem, setProblem] = useState(null);
    const [language, setLanguage] = useState('pyspark');
    const [myCode, setMyCode] = useState('');
    const [opponentCode, setOpponentCode] = useState('');
    const [activeTab, setActiveTab] = useState('mine');
    const [executing, setExecuting] = useState(false);
    const [submitting, setSubmitting] = useState(false);
    const [myResult, setMyResult] = useState(null);
    const [opponentResult, setOpponentResult] = useState(null);
    const [connectedUsers, setConnectedUsers] = useState([]);
    const [opponentUsername, setOpponentUsername] = useState('');

    // --- Problem Selection State (full sandbox-style) ---
    const [showProblemPicker, setShowProblemPicker] = useState(false);
    const [generatingProblem, setGeneratingProblem] = useState(false);
    const [selectedMode, setSelectedMode] = useState('pyspark');
    const [selectedDifficulty, setSelectedDifficulty] = useState('Easy');
    const [selectedTags, setSelectedTags] = useState([]);   // [{topic, subtopics: []}]
    const [activeTopic, setActiveTopic] = useState(null);   // topic with subtopic picker open
    const [subtopicSuggestions, setSubtopicSuggestions] = useState([]);
    const [loadingSubtopics, setLoadingSubtopics] = useState(false);

    // --- Chat State ---
    const [chatMessages, setChatMessages] = useState([]);
    const [chatInput, setChatInput] = useState('');
    const [showChat, setShowChat] = useState(false);
    const [unreadCount, setUnreadCount] = useState(0);
    const chatEndRef = useRef(null);

    // --- Voice State ---
    const [voiceActive, setVoiceActive] = useState(false);
    const [isMuted, setIsMuted] = useState(false);
    const peerConnectionRef = useRef(null);
    const localStreamRef = useRef(null);
    const remoteAudioRef = useRef(null);

    // WebSocket ref
    const wsRef = useRef(null);
    const codeUpdateTimer = useRef(null);

    // Current topics list based on mode
    const currentTopics = selectedMode === 'sql' ? SQL_TOPICS : TOPICS;

    // Determine opponent's username
    useEffect(() => {
        if (roomInfo && user) {
            const opp = roomInfo.creator === user.username ? roomInfo.joiner : roomInfo.creator;
            setOpponentUsername(opp || '');
        }
    }, [roomInfo, user]);

    // --- WebSocket Connection ---
    const connectWebSocket = useCallback((code) => {
        if (!token || !code) return;

        const ws = new WebSocket(`${WS_BASE_URL}/ws/room/${code}?token=${token}`);
        wsRef.current = ws;

        ws.onopen = () => {
            console.log('WebSocket connected to room', code);
        };

        ws.onmessage = (event) => {
            const msg = JSON.parse(event.data);

            switch (msg.type) {
                case 'room_state':
                    setConnectedUsers(msg.connected_users || []);
                    if (msg.problem) {
                        setProblem(msg.problem);
                        setLanguage(msg.language || 'pyspark');
                        setMyCode(msg.problem.initial_code || '');
                        setPhase('challenge');
                    }
                    if (msg.opponent_code) {
                        setOpponentCode(msg.opponent_code);
                    }
                    break;

                case 'player_connected':
                    setConnectedUsers(msg.connected_users || []);
                    setRoomInfo(prev => prev ? { ...prev, joiner: msg.username, status: 'active' } : prev);
                    break;

                case 'player_disconnected':
                    setConnectedUsers(msg.connected_users || []);
                    break;

                case 'room_closed':
                    // Opponent left — room is deleted. Show message and go back to lobby.
                    setRoomClosedMsg(msg.message || 'Opponent left. Room closed.');
                    setPhase('lobby');
                    setRoomCode('');
                    setRoomInfo(null);
                    setProblem(null);
                    setMyCode('');
                    setOpponentCode('');
                    setConnectedUsers([]);
                    break;

                case 'opponent_code':
                    setOpponentCode(msg.code);
                    break;

                case 'problem_set':
                    setProblem(msg.problem);
                    setLanguage(msg.language || 'pyspark');
                    setMyCode(msg.problem.initial_code || '');
                    setMyResult(null);
                    setOpponentResult(null);
                    setOpponentCode('');
                    setShowProblemPicker(false);
                    setPhase('challenge');
                    break;

                case 'opponent_run_result':
                    setOpponentResult({ ...msg.result, type: 'run' });
                    break;

                case 'opponent_submit_result':
                    setOpponentResult({ ...msg.result, type: 'submit' });
                    break;

                case 'chat_message':
                    setChatMessages(prev => [...prev, {
                        sender: msg.username,
                        message: msg.message,
                        timestamp: msg.timestamp,
                        isMe: false
                    }]);
                    setUnreadCount(prev => prev + 1);
                    break;

                // WebRTC signaling
                case 'webrtc_offer':
                    handleWebRTCOffer(msg.data);
                    break;
                case 'webrtc_answer':
                    handleWebRTCAnswer(msg.data);
                    break;
                case 'webrtc_ice_candidate':
                    handleWebRTCIceCandidate(msg.data);
                    break;
            }
        };

        ws.onclose = () => {
            console.log('WebSocket disconnected');
        };

        return () => {
            ws.close();
        };
    }, [token]);

    // Auto-scroll chat
    useEffect(() => {
        if (chatEndRef.current) chatEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }, [chatMessages]);

    // Reset unread when chat is opened
    useEffect(() => {
        if (showChat) setUnreadCount(0);
    }, [showChat]);

    // Cleanup on unmount
    useEffect(() => {
        return () => {
            if (wsRef.current) wsRef.current.close();
            if (codeUpdateTimer.current) clearTimeout(codeUpdateTimer.current);
            endVoiceCall();
        };
    }, []);

    // --- Room Actions ---
    const createRoom = async () => {
        setError('');
        setRoomClosedMsg('');
        try {
            const res = await axios.post(`${API_BASE_URL}/api/room/create`, {}, {
                headers: { Authorization: `Bearer ${token}` }
            });
            setRoomCode(res.data.room_code);
            setRoomInfo({ creator: res.data.creator, joiner: null, status: 'waiting' });
            setPhase('waiting');
            connectWebSocket(res.data.room_code);
        } catch (err) {
            setError(err.response?.data?.detail || 'Failed to create room.');
        }
    };

    const joinRoom = async () => {
        setError('');
        setRoomClosedMsg('');
        const code = joinInput.trim().toUpperCase();
        if (!code || code.length < 4) {
            setError('Please enter a valid room code.');
            return;
        }
        try {
            const res = await axios.post(`${API_BASE_URL}/api/room/join`, { room_code: code }, {
                headers: { Authorization: `Bearer ${token}` }
            });
            setRoomCode(res.data.room_code);
            setRoomInfo({
                creator: res.data.creator,
                joiner: res.data.joiner,
                status: res.data.status
            });
            setPhase('waiting');
            connectWebSocket(res.data.room_code);
        } catch (err) {
            setError(err.response?.data?.detail || 'Failed to join room.');
        }
    };

    const copyRoomCode = () => {
        navigator.clipboard.writeText(roomCode);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
    };

    const leaveRoom = () => {
        endVoiceCall();
        if (wsRef.current) wsRef.current.close();
        setPhase('lobby');
        setRoomCode('');
        setRoomInfo(null);
        setProblem(null);
        setMyCode('');
        setOpponentCode('');
        setMyResult(null);
        setOpponentResult(null);
        setConnectedUsers([]);
        setChatMessages([]);
    };

    // --- Chat ---
    const sendChatMessage = () => {
        const msg = chatInput.trim();
        if (!msg || !wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) return;
        wsRef.current.send(JSON.stringify({ type: 'chat_message', message: msg }));
        setChatMessages(prev => [...prev, {
            sender: user?.username,
            message: msg,
            timestamp: new Date().toISOString(),
            isMe: true
        }]);
        setChatInput('');
    };

    // --- Voice (WebRTC) ---
    const ICE_SERVERS = { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] };

    const createPeerConnection = () => {
        const pc = new RTCPeerConnection(ICE_SERVERS);

        pc.onicecandidate = (event) => {
            if (event.candidate && wsRef.current?.readyState === WebSocket.OPEN) {
                wsRef.current.send(JSON.stringify({
                    type: 'webrtc_ice_candidate',
                    data: event.candidate
                }));
            }
        };

        pc.ontrack = (event) => {
            if (remoteAudioRef.current) {
                remoteAudioRef.current.srcObject = event.streams[0];
            }
        };

        peerConnectionRef.current = pc;
        return pc;
    };

    const startVoiceCall = async () => {
        try {
            const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
            localStreamRef.current = stream;

            const pc = createPeerConnection();
            stream.getTracks().forEach(track => pc.addTrack(track, stream));

            const offer = await pc.createOffer();
            await pc.setLocalDescription(offer);

            if (wsRef.current?.readyState === WebSocket.OPEN) {
                wsRef.current.send(JSON.stringify({
                    type: 'webrtc_offer',
                    data: offer
                }));
            }

            setVoiceActive(true);
            setIsMuted(false);
        } catch (err) {
            console.error('Failed to start voice:', err);
            setError('Could not access microphone.');
        }
    };

    const handleWebRTCOffer = async (offer) => {
        try {
            const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
            localStreamRef.current = stream;

            const pc = createPeerConnection();
            stream.getTracks().forEach(track => pc.addTrack(track, stream));

            await pc.setRemoteDescription(new RTCSessionDescription(offer));
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);

            if (wsRef.current?.readyState === WebSocket.OPEN) {
                wsRef.current.send(JSON.stringify({
                    type: 'webrtc_answer',
                    data: answer
                }));
            }

            setVoiceActive(true);
            setIsMuted(false);
        } catch (err) {
            console.error('Failed to answer voice call:', err);
        }
    };

    const handleWebRTCAnswer = async (answer) => {
        if (peerConnectionRef.current) {
            await peerConnectionRef.current.setRemoteDescription(new RTCSessionDescription(answer));
        }
    };

    const handleWebRTCIceCandidate = async (candidate) => {
        if (peerConnectionRef.current) {
            try {
                await peerConnectionRef.current.addIceCandidate(new RTCIceCandidate(candidate));
            } catch (err) {
                console.error('ICE candidate error:', err);
            }
        }
    };

    const endVoiceCall = () => {
        if (peerConnectionRef.current) {
            peerConnectionRef.current.close();
            peerConnectionRef.current = null;
        }
        if (localStreamRef.current) {
            localStreamRef.current.getTracks().forEach(t => t.stop());
            localStreamRef.current = null;
        }
        setVoiceActive(false);
        setIsMuted(false);
    };

    const toggleMute = () => {
        if (localStreamRef.current) {
            const audioTrack = localStreamRef.current.getAudioTracks()[0];
            if (audioTrack) {
                audioTrack.enabled = !audioTrack.enabled;
                setIsMuted(!audioTrack.enabled);
            }
        }
    };

    // --- Code Sync ---
    const handleCodeChange = (value) => {
        setMyCode(value);
        if (codeUpdateTimer.current) clearTimeout(codeUpdateTimer.current);
        codeUpdateTimer.current = setTimeout(() => {
            if (wsRef.current?.readyState === WebSocket.OPEN) {
                wsRef.current.send(JSON.stringify({ type: 'code_update', code: value }));
            }
        }, 300);
    };

    // --- Topic / Subtopic Selection (matching sandbox) ---
    const toggleTopic = async (topic) => {
        const isSelected = selectedTags.some(t => t.topic.id === topic.id);
        if (isSelected) {
            // Deselect
            setSelectedTags(prev => prev.filter(t => t.topic.id !== topic.id));
            if (activeTopic?.id === topic.id) {
                setActiveTopic(null);
                setSubtopicSuggestions([]);
            }
        } else {
            // Select and fetch subtopics
            setSelectedTags(prev => [...prev, { topic, subtopics: [] }]);
            setActiveTopic(topic);
            setLoadingSubtopics(true);
            try {
                const endpoint = selectedMode === 'sql'
                    ? `${API_BASE_URL}/api/sql/subtopics`
                    : `${API_BASE_URL}/api/subtopics`;
                const res = await axios.get(endpoint, { params: { topic: topic.label } });
                setSubtopicSuggestions(res.data.subtopics || []);
            } catch {
                setSubtopicSuggestions([]);
            } finally {
                setLoadingSubtopics(false);
            }
        }
    };

    const toggleSubtopic = (subtopicLabel) => {
        if (!activeTopic) return;
        setSelectedTags(prev => prev.map(item => {
            if (item.topic.id === activeTopic.id) {
                const has = item.subtopics.includes(subtopicLabel);
                return {
                    ...item,
                    subtopics: has
                        ? item.subtopics.filter(s => s !== subtopicLabel)
                        : [...item.subtopics, subtopicLabel]
                };
            }
            return item;
        }));
    };

    // --- Problem Generation ---
    const generateProblem = async () => {
        setGeneratingProblem(true);
        setError('');
        try {
            // Build topic string from selected tags (same logic as sandbox)
            const combinedTopicsList = selectedTags.map(t => {
                if (t.subtopics.length > 0) {
                    return `${t.topic.label} (${t.subtopics.join(", ")})`;
                }
                return t.topic.label;
            });
            const combinedTopics = combinedTopicsList.length > 0 ? combinedTopicsList.join("; ") : 'general';

            const endpoint = selectedMode === 'sql'
                ? `${API_BASE_URL}/api/sql/problem/generate`
                : `${API_BASE_URL}/api/problem/generate`;
            const res = await axios.get(endpoint, {
                params: { topic: combinedTopics, difficulty: selectedDifficulty }
            });

            // Send problem to both players via WebSocket
            if (wsRef.current?.readyState === WebSocket.OPEN) {
                wsRef.current.send(JSON.stringify({
                    type: 'set_problem',
                    problem: res.data,
                    language: selectedMode
                }));
            }
        } catch (err) {
            setError('Failed to generate problem.');
        } finally {
            setGeneratingProblem(false);
        }
    };

    // --- Run / Submit ---
    const runCode = async () => {
        if (!problem || executing) return;
        setExecuting(true);
        setMyResult(null);
        try {
            const endpoint = language === 'sql'
                ? `${API_BASE_URL}/api/sql/problem/execute`
                : `${API_BASE_URL}/api/problem/execute`;
            const payload = language === 'sql'
                ? { code: myCode, datasets: problem.datasets, language: 'sql' }
                : { code: myCode, datasets: problem.datasets };
            const res = await axios.post(endpoint, payload);
            const result = { ...res.data, type: 'run' };
            setMyResult(result);
            if (wsRef.current?.readyState === WebSocket.OPEN) {
                wsRef.current.send(JSON.stringify({ type: 'run_result', result }));
            }
        } catch (err) {
            setMyResult({ success: false, error: 'Execution error', type: 'run' });
        } finally {
            setExecuting(false);
        }
    };

    const submitCode = async () => {
        if (!problem || submitting) return;
        setSubmitting(true);
        setMyResult(null);
        try {
            const endpoint = language === 'sql'
                ? `${API_BASE_URL}/api/sql/problem/submit`
                : `${API_BASE_URL}/api/problem/submit`;
            const payload = {
                code: myCode,
                datasets: problem.datasets,
                expected_output: problem.expected_output,
                difficulty: problem.difficulty || 'Easy',
                title: problem.title || 'Twin Challenge'
            };
            const res = await axios.post(endpoint, payload, {
                headers: token ? { Authorization: `Bearer ${token}` } : {}
            });
            const result = { ...res.data, type: 'submit' };
            setMyResult(result);
            if (wsRef.current?.readyState === WebSocket.OPEN) {
                wsRef.current.send(JSON.stringify({ type: 'submit_result', result }));
            }
        } catch (err) {
            setMyResult({ success: false, passed: false, message: 'Submission error', type: 'submit' });
        } finally {
            setSubmitting(false);
        }
    };

    // --- Monaco editor theme ---
    const handleEditorBeforeMount = (monaco) => {
        monaco.editor.defineTheme('twin-theme', {
            base: 'vs-dark',
            inherit: true,
            rules: [],
            colors: {
                'editor.background': '#0d1117',
                'editor.foreground': '#e6edf3',
            }
        });
    };

    const isOpponentConnected = connectedUsers.length >= 2;

    // Group topics by category for rendering
    const groupedTopics = currentTopics.reduce((acc, topic) => {
        if (!acc[topic.category]) acc[topic.category] = [];
        acc[topic.category].push(topic);
        return acc;
    }, {});

    // --- Shared Problem Picker UI ---
    const renderProblemPicker = (isInline = false) => (
        <div className={isInline ? "twin-quick-picker fade-in" : "twin-problem-picker fade-in"}>
            <div className="twin-picker-section">
                <div className="twin-picker-row">
                    <label>Mode</label>
                    <div className="mode-toggle">
                        <button className={`mode-btn ${selectedMode === 'pyspark' ? 'active' : ''}`} onClick={() => { setSelectedMode('pyspark'); setSelectedTags([]); setActiveTopic(null); setSubtopicSuggestions([]); }}>PySpark</button>
                        <button className={`mode-btn ${selectedMode === 'sql' ? 'active' : ''}`} onClick={() => { setSelectedMode('sql'); setSelectedTags([]); setActiveTopic(null); setSubtopicSuggestions([]); }}>SQL</button>
                    </div>
                </div>

                <div className="twin-picker-row">
                    <label>Difficulty</label>
                    <div className="segmented-control">
                        {['Easy', 'Medium', 'Hard'].map(d => (
                            <button key={d} className={`segment-btn ${selectedDifficulty === d ? 'active' : ''}`} onClick={() => setSelectedDifficulty(d)}>{d}</button>
                        ))}
                    </div>
                </div>

                <div className="twin-topics-section">
                    <label><Tags size={14} /> Topics {selectedTags.length > 0 && <span className="twin-tag-count">{selectedTags.length}</span>}</label>
                    {Object.entries(groupedTopics).map(([category, topics]) => (
                        <div key={category} className="twin-topic-group">
                            <span className="twin-topic-category">{category}</span>
                            <div className="pill-container">
                                {topics.map(topic => (
                                    <button
                                        key={topic.id}
                                        className={`pill ${selectedTags.some(t => t.topic.id === topic.id) ? 'active' : ''}`}
                                        onClick={() => toggleTopic(topic)}
                                    >
                                        {topic.label}
                                    </button>
                                ))}
                            </div>
                        </div>
                    ))}
                </div>

                {/* Subtopic suggestions */}
                {activeTopic && (
                    <div className="twin-subtopics-section fade-in">
                        <label>Subtopics for <strong>{activeTopic.label}</strong></label>
                        {loadingSubtopics ? (
                            <div className="twin-subtopic-loading"><Loader2 size={14} className="spinner" /> Loading subtopics...</div>
                        ) : (
                            <div className="pill-container">
                                {subtopicSuggestions.map(sub => {
                                    const tagEntry = selectedTags.find(t => t.topic.id === activeTopic.id);
                                    const isSelected = tagEntry?.subtopics.includes(sub);
                                    return (
                                        <button
                                            key={sub}
                                            className={`pill sub-pill ${isSelected ? 'selected' : ''}`}
                                            onClick={() => toggleSubtopic(sub)}
                                        >
                                            {sub}
                                        </button>
                                    );
                                })}
                            </div>
                        )}
                    </div>
                )}

                {/* Selected summary */}
                {selectedTags.length > 0 && (
                    <div className="twin-selected-summary">
                        {selectedTags.map(tag => (
                            <span key={tag.topic.id} className="twin-selected-tag">
                                {tag.topic.label}
                                {tag.subtopics.length > 0 && ` (${tag.subtopics.join(', ')})`}
                                <X size={12} className="twin-tag-remove" onClick={() => toggleTopic(tag.topic)} />
                            </span>
                        ))}
                    </div>
                )}
            </div>

            <button className="btn btn-generate twin-start-btn" onClick={generateProblem} disabled={generatingProblem}>
                {generatingProblem ? <><Loader2 size={16} className="spinner" /> Generating...</> : <><Swords size={16} /> {problem ? 'New Problem' : 'Start Challenge'}</>}
            </button>
        </div>
    );

    // =================== RENDER ===================

    // LOBBY PHASE
    if (phase === 'lobby') {
        return (
            <div className="twin-container">
                <div className="twin-lobby">
                    <button className="twin-back-btn" onClick={onBack}>
                        <ArrowLeft size={18} /> Back to Home
                    </button>
                    <div className="twin-lobby-hero">
                        <div className="twin-lobby-icon"><Swords size={48} /></div>
                        <h1>Twin Challenge</h1>
                        <p>Challenge a friend to solve the same problem. Code side-by-side and peek at their strategy in real-time.</p>
                    </div>

                    {roomClosedMsg && <div className="twin-info-msg">{roomClosedMsg}</div>}
                    {error && <div className="twin-error">{error}</div>}

                    <div className="twin-lobby-actions">
                        <div className="twin-lobby-card" onClick={createRoom}>
                            <Crown size={28} className="twin-card-icon" />
                            <h3>Create Room</h3>
                            <p>Start a new room and invite your friend with a code</p>
                        </div>

                        <div className="twin-lobby-divider">OR</div>

                        <div className="twin-lobby-card twin-join-card">
                            <UserPlus size={28} className="twin-card-icon" />
                            <h3>Join Room</h3>
                            <div className="twin-join-input-row">
                                <input
                                    type="text"
                                    placeholder="Enter room code"
                                    value={joinInput}
                                    onChange={(e) => setJoinInput(e.target.value.toUpperCase())}
                                    maxLength={6}
                                    className="twin-join-input"
                                    onKeyDown={(e) => e.key === 'Enter' && joinRoom()}
                                />
                                <button className="twin-join-btn" onClick={joinRoom}>Join</button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        );
    }

    // WAITING PHASE
    if (phase === 'waiting') {
        return (
            <div className="twin-container">
                <div className="twin-waiting">
                    <button className="twin-back-btn" onClick={leaveRoom}>
                        <ArrowLeft size={18} /> Leave Room
                    </button>

                    <div className="twin-waiting-content">
                        <div className="twin-room-code-display">
                            <span className="twin-room-label">Room Code</span>
                            <div className="twin-room-code-row">
                                <span className="twin-room-code">{roomCode}</span>
                                <button className="twin-copy-btn" onClick={copyRoomCode}>
                                    {copied ? <Check size={16} /> : <Copy size={16} />}
                                    {copied ? 'Copied!' : 'Copy'}
                                </button>
                            </div>
                        </div>

                        <div className="twin-players">
                            <div className="twin-player-slot twin-player-filled">
                                <div className="twin-player-avatar">{user?.username?.[0]?.toUpperCase()}</div>
                                <span>{user?.username} (You)</span>
                                <Wifi size={14} className="twin-online-icon" />
                            </div>

                            <div className="twin-vs">VS</div>

                            {isOpponentConnected ? (
                                <div className="twin-player-slot twin-player-filled">
                                    <div className="twin-player-avatar twin-opponent-avatar">{opponentUsername?.[0]?.toUpperCase()}</div>
                                    <span>{opponentUsername}</span>
                                    <Wifi size={14} className="twin-online-icon" />
                                </div>
                            ) : (
                                <div className="twin-player-slot twin-player-empty">
                                    <div className="twin-waiting-pulse"></div>
                                    <span>Waiting for opponent...</span>
                                </div>
                            )}
                        </div>

                        {isOpponentConnected && (
                            <div className="twin-problem-picker-area">
                                {!showProblemPicker ? (
                                    <button className="btn btn-generate twin-generate-btn" onClick={() => setShowProblemPicker(true)}>
                                        <Swords size={18} /> Pick a Problem
                                    </button>
                                ) : (
                                    renderProblemPicker()
                                )}
                                {error && <div className="twin-error" style={{ marginTop: '1rem' }}>{error}</div>}
                            </div>
                        )}
                    </div>
                </div>
            </div>
        );
    }

    // CHALLENGE PHASE
    return (
        <div className="twin-challenge-container">
            {/* Header Bar */}
            <div className="twin-challenge-header">
                <div className="twin-header-left">
                    <button className="icon-btn" onClick={leaveRoom} title="Leave Room">
                        <LogOut size={18} />
                    </button>
                    <span className="twin-room-badge">Room: {roomCode}</span>
                    <span className={`twin-lang-badge ${language === 'sql' ? 'lang-badge-sql' : 'lang-badge-pyspark'}`}>{language.toUpperCase()}</span>
                </div>
                <div className="twin-header-center">
                    <div className="twin-header-player">
                        <div className="twin-mini-avatar">{user?.username?.[0]?.toUpperCase()}</div>
                        <span>{user?.username}</span>
                    </div>
                    <span className="twin-header-vs">VS</span>
                    <div className="twin-header-player">
                        <div className="twin-mini-avatar twin-opponent-avatar">{opponentUsername?.[0]?.toUpperCase() || '?'}</div>
                        <span>{opponentUsername || 'Opponent'}</span>
                        {isOpponentConnected ? <Wifi size={12} className="twin-online-icon" /> : <WifiOff size={12} className="twin-offline-icon" />}
                    </div>
                </div>
                <div className="twin-header-right">
                    <button className="btn btn-submit" onClick={() => setShowProblemPicker(!showProblemPicker)}>
                        New Problem
                    </button>
                </div>
            </div>

            {/* Inline Problem Picker Dropdown */}
            {showProblemPicker && renderProblemPicker(true)}

            <div className="twin-challenge-body">
                {/* Left Panel — Problem Description */}
                <div className="twin-left-panel">
                    <div className="panel-header">
                        <h2>{problem?.title || 'Challenge Problem'}</h2>
                        <span className="badge">{problem?.difficulty || 'Easy'}</span>
                    </div>
                    <div className="problem-content">
                        <div className="problem-description">{problem?.description || 'Waiting for problem...'}</div>

                        {problem?.datasets && Object.entries(problem.datasets).map(([name, rows]) => (
                            <div key={name}>
                                <h4 className="dataset-title">{name}</h4>
                                {Array.isArray(rows) && rows.length > 0 && (
                                    <table className="dataset-table">
                                        <thead>
                                            <tr>{Object.keys(rows[0]).map(k => <th key={k}>{k}</th>)}</tr>
                                        </thead>
                                        <tbody>
                                            {rows.slice(0, 8).map((row, i) => (
                                                <tr key={i}>{Object.values(row).map((v, j) => <td key={j}>{String(v)}</td>)}</tr>
                                            ))}
                                        </tbody>
                                    </table>
                                )}
                            </div>
                        ))}

                        {problem?.expected_output && (
                            <>
                                <h4 className="dataset-title">Expected Output</h4>
                                <table className="dataset-table">
                                    <thead>
                                        <tr>{Object.keys(problem.expected_output[0] || {}).map(k => <th key={k}>{k}</th>)}</tr>
                                    </thead>
                                    <tbody>
                                        {problem.expected_output.slice(0, 8).map((row, i) => (
                                            <tr key={i}>{Object.values(row).map((v, j) => <td key={j}>{String(v)}</td>)}</tr>
                                        ))}
                                    </tbody>
                                </table>
                            </>
                        )}
                    </div>
                </div>

                {/* Right Panel — Tabbed Editor */}
                <div className="twin-right-panel">
                    <div className="twin-tab-bar">
                        <button className={`twin-tab ${activeTab === 'mine' ? 'active' : ''}`} onClick={() => setActiveTab('mine')}>
                            <div className="twin-mini-avatar" style={{ width: 22, height: 22, fontSize: '0.65rem' }}>{user?.username?.[0]?.toUpperCase()}</div>
                            Your Code
                        </button>
                        <button className={`twin-tab ${activeTab === 'opponent' ? 'active' : ''}`} onClick={() => setActiveTab('opponent')}>
                            <div className="twin-mini-avatar twin-opponent-avatar" style={{ width: 22, height: 22, fontSize: '0.65rem' }}>{opponentUsername?.[0]?.toUpperCase() || '?'}</div>
                            {opponentUsername ? `${opponentUsername}'s Code` : "Opponent's Code"}
                            <span className="twin-live-dot"></span>
                        </button>
                    </div>

                    <div className="twin-editor-area">
                        {activeTab === 'mine' && (
                            <div className="twin-editor-wrapper">
                                <div className="panel-header">
                                    <h2>{language === 'sql' ? 'query.sql' : 'main.py'}</h2>
                                    <div style={{ display: 'flex', gap: '10px' }}>
                                        <button className="btn btn-success" onClick={runCode} disabled={executing || submitting}>
                                            {executing ? <Loader2 size={16} className="spinner" /> : <><Play size={16} /> Run</>}
                                        </button>
                                        <button className="btn btn-submit" onClick={submitCode} disabled={executing || submitting}>
                                            {submitting ? <Loader2 size={16} className="spinner" /> : <><Send size={16} /> Submit</>}
                                        </button>
                                    </div>
                                </div>
                                <Editor
                                    height="100%"
                                    language={language === 'sql' ? 'sql' : 'python'}
                                    theme="twin-theme"
                                    beforeMount={handleEditorBeforeMount}
                                    value={myCode}
                                    onChange={handleCodeChange}
                                    options={{
                                        minimap: { enabled: false },
                                        fontSize: 14,
                                        fontFamily: "'Consolas', 'Courier New', monospace",
                                        scrollBeyondLastLine: false,
                                    }}
                                />
                            </div>
                        )}

                        {activeTab === 'opponent' && (
                            <div className="twin-editor-wrapper">
                                <div className="panel-header">
                                    <h2>
                                        <span className="twin-live-indicator">LIVE</span>
                                        {opponentUsername ? `${opponentUsername}'s Code` : "Opponent's Code"}
                                    </h2>
                                </div>
                                <Editor
                                    height="100%"
                                    language={language === 'sql' ? 'sql' : 'python'}
                                    theme="twin-theme"
                                    beforeMount={handleEditorBeforeMount}
                                    value={opponentCode || '// Waiting for opponent to start typing...'}
                                    options={{
                                        readOnly: true,
                                        minimap: { enabled: false },
                                        fontSize: 14,
                                        fontFamily: "'Consolas', 'Courier New', monospace",
                                        scrollBeyondLastLine: false,
                                    }}
                                />
                            </div>
                        )}
                    </div>

                    {/* Results Panel */}
                    <div className="twin-results-panel">
                        <div className="panel-header">
                            <h2>Results</h2>
                            {myResult && (
                                <span className={myResult.success ? 'success-text' : 'error-text'}>
                                    {myResult.type === 'submit'
                                        ? (myResult.passed ? '✅ Passed!' : '❌ Failed')
                                        : (myResult.success ? '✓ Success' : 'Error')}
                                </span>
                            )}
                        </div>
                        <div className="twin-results-content">
                            <div className="twin-results-col">
                                <h4>Your Result</h4>
                                {!myResult ? (
                                    <span className="twin-result-empty">Run your code to see results...</span>
                                ) : (
                                    <>
                                        {myResult.output && <pre className="output-pre">{myResult.output}</pre>}
                                        {myResult.error && <pre className="error-text">{myResult.error}</pre>}
                                        {myResult.message && <p style={{ color: myResult.passed ? 'var(--success)' : 'var(--error)' }}>{myResult.message}</p>}
                                    </>
                                )}
                            </div>
                            <div className="twin-results-divider"></div>
                            <div className="twin-results-col">
                                <h4>{opponentUsername || 'Opponent'}'s Result</h4>
                                {!opponentResult ? (
                                    <span className="twin-result-empty">Waiting for opponent...</span>
                                ) : (
                                    <>
                                        {opponentResult.type === 'submit' && (
                                            <p style={{ color: opponentResult.passed ? 'var(--success)' : 'var(--error)' }}>
                                                {opponentResult.passed ? '✅ Passed!' : `❌ ${opponentResult.message || 'Failed'}`}
                                            </p>
                                        )}
                                        {opponentResult.type === 'run' && (
                                            <p style={{ color: opponentResult.success ? 'var(--success)' : 'var(--error)' }}>
                                                {opponentResult.success ? '✓ Code ran successfully' : 'Error in execution'}
                                            </p>
                                        )}
                                    </>
                                )}
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Hidden audio element for WebRTC remote audio */}
            <audio ref={remoteAudioRef} autoPlay />

            {/* Voice Controls (floating) */}
            <div className="twin-voice-controls">
                {!voiceActive ? (
                    <button className="twin-voice-btn twin-voice-start" onClick={startVoiceCall} title="Start Voice Call">
                        <Phone size={18} />
                    </button>
                ) : (
                    <>
                        <button className={`twin-voice-btn ${isMuted ? 'twin-voice-muted' : 'twin-voice-active'}`} onClick={toggleMute} title={isMuted ? 'Unmute' : 'Mute'}>
                            {isMuted ? <MicOff size={18} /> : <Mic size={18} />}
                        </button>
                        <button className="twin-voice-btn twin-voice-end" onClick={endVoiceCall} title="End Call">
                            <PhoneOff size={18} />
                        </button>
                    </>
                )}
            </div>

            {/* Chat Toggle Button */}
            <button className="twin-chat-toggle" onClick={() => setShowChat(!showChat)}>
                <MessageCircle size={20} />
                {unreadCount > 0 && <span className="twin-chat-badge">{unreadCount}</span>}
            </button>

            {/* Chat Panel (slide-in) */}
            {showChat && (
                <div className="twin-chat-panel fade-in">
                    <div className="twin-chat-header">
                        <h3><MessageCircle size={16} /> Chat</h3>
                        <button className="icon-btn" onClick={() => setShowChat(false)}><X size={16} /></button>
                    </div>
                    <div className="twin-chat-messages">
                        {chatMessages.length === 0 && (
                            <div className="twin-chat-empty">No messages yet. Say hi! 👋</div>
                        )}
                        {chatMessages.map((msg, i) => (
                            <div key={i} className={`twin-chat-msg ${msg.isMe ? 'twin-chat-mine' : 'twin-chat-theirs'}`}>
                                <span className="twin-chat-sender">{msg.isMe ? 'You' : msg.sender}</span>
                                <span className="twin-chat-text">{msg.message}</span>
                            </div>
                        ))}
                        <div ref={chatEndRef} />
                    </div>
                    <div className="twin-chat-input-row">
                        <input
                            type="text"
                            value={chatInput}
                            onChange={(e) => setChatInput(e.target.value)}
                            onKeyDown={(e) => e.key === 'Enter' && sendChatMessage()}
                            placeholder="Type a message..."
                            className="twin-chat-input"
                        />
                        <button className="twin-chat-send" onClick={sendChatMessage}>
                            <Send size={16} />
                        </button>
                    </div>
                </div>
            )}
        </div>
    );
}

export default TwinChallenge;
