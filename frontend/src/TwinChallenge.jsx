import { useState, useEffect, useRef, useCallback, useContext } from 'react';
import { AuthContext } from './AuthContext';
import { API_BASE_URL } from './config';
import Editor from '@monaco-editor/react';
import axios from 'axios';
import {
    Copy, Check, Users, ArrowLeft, Play, Loader2, Send, Wifi, WifiOff,
    Swords, Crown, UserPlus, LogOut
} from 'lucide-react';
import './TwinChallenge.css';

// WebSocket URL from API_BASE_URL (convert http→ws, https→wss)
const WS_BASE_URL = API_BASE_URL.replace(/^http/, 'ws');

function TwinChallenge({ onBack }) {
    const { user, token } = useContext(AuthContext);

    // --- Room State ---
    const [phase, setPhase] = useState('lobby'); // lobby | waiting | challenge
    const [roomCode, setRoomCode] = useState('');
    const [joinInput, setJoinInput] = useState('');
    const [roomInfo, setRoomInfo] = useState(null); // { creator, joiner, status }
    const [error, setError] = useState('');
    const [copied, setCopied] = useState(false);

    // --- Challenge State ---
    const [problem, setProblem] = useState(null);
    const [language, setLanguage] = useState('pyspark');
    const [myCode, setMyCode] = useState('');
    const [opponentCode, setOpponentCode] = useState('');
    const [activeTab, setActiveTab] = useState('mine'); // mine | opponent
    const [executing, setExecuting] = useState(false);
    const [submitting, setSubmitting] = useState(false);
    const [myResult, setMyResult] = useState(null);
    const [opponentResult, setOpponentResult] = useState(null);
    const [connectedUsers, setConnectedUsers] = useState([]);
    const [opponentUsername, setOpponentUsername] = useState('');

    // --- Problem Selection State ---
    const [showProblemPicker, setShowProblemPicker] = useState(false);
    const [generatingProblem, setGeneratingProblem] = useState(false);
    const [selectedMode, setSelectedMode] = useState('pyspark');
    const [selectedDifficulty, setSelectedDifficulty] = useState('Easy');

    // WebSocket ref
    const wsRef = useRef(null);
    const codeUpdateTimer = useRef(null);

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
            }
        };

        ws.onclose = () => {
            console.log('WebSocket disconnected');
        };

        return () => {
            ws.close();
        };
    }, [token]);

    // Cleanup on unmount
    useEffect(() => {
        return () => {
            if (wsRef.current) wsRef.current.close();
            if (codeUpdateTimer.current) clearTimeout(codeUpdateTimer.current);
        };
    }, []);

    // --- Room Actions ---
    const createRoom = async () => {
        setError('');
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
            setPhase(res.data.status === 'active' ? 'waiting' : 'waiting');
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
    };

    // --- Code Sync ---
    const handleCodeChange = (value) => {
        setMyCode(value);
        // Debounce code updates to avoid flooding WS
        if (codeUpdateTimer.current) clearTimeout(codeUpdateTimer.current);
        codeUpdateTimer.current = setTimeout(() => {
            if (wsRef.current?.readyState === WebSocket.OPEN) {
                wsRef.current.send(JSON.stringify({ type: 'code_update', code: value }));
            }
        }, 300);
    };

    // --- Problem Generation ---
    const generateProblem = async () => {
        setGeneratingProblem(true);
        try {
            const endpoint = selectedMode === 'sql'
                ? `${API_BASE_URL}/api/sql/problem/generate`
                : `${API_BASE_URL}/api/problem/generate`;
            const res = await axios.get(endpoint, {
                params: { topic: 'general', difficulty: selectedDifficulty }
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
            // Share result with opponent
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
                            <div className="twin-problem-picker-area fade-in">
                                {!showProblemPicker ? (
                                    <button className="btn btn-generate twin-generate-btn" onClick={() => setShowProblemPicker(true)}>
                                        <Swords size={18} /> Pick a Problem
                                    </button>
                                ) : (
                                    <div className="twin-problem-picker">
                                        <h3>Configure Challenge</h3>
                                        <div className="twin-picker-row">
                                            <label>Mode</label>
                                            <div className="mode-toggle">
                                                <button className={`mode-btn ${selectedMode === 'pyspark' ? 'active' : ''}`} onClick={() => setSelectedMode('pyspark')}>PySpark</button>
                                                <button className={`mode-btn ${selectedMode === 'sql' ? 'active' : ''}`} onClick={() => setSelectedMode('sql')}>SQL</button>
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
                                        <button className="btn btn-generate twin-start-btn" onClick={generateProblem} disabled={generatingProblem}>
                                            {generatingProblem ? <><Loader2 size={16} className="spinner" /> Generating...</> : <><Swords size={16} /> Start Challenge</>}
                                        </button>
                                    </div>
                                )}
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

            {/* Quick Problem Picker Dropdown */}
            {showProblemPicker && (
                <div className="twin-quick-picker fade-in">
                    <div className="twin-picker-row">
                        <div className="mode-toggle">
                            <button className={`mode-btn ${selectedMode === 'pyspark' ? 'active' : ''}`} onClick={() => setSelectedMode('pyspark')}>PySpark</button>
                            <button className={`mode-btn ${selectedMode === 'sql' ? 'active' : ''}`} onClick={() => setSelectedMode('sql')}>SQL</button>
                        </div>
                        <div className="segmented-control">
                            {['Easy', 'Medium', 'Hard'].map(d => (
                                <button key={d} className={`segment-btn ${selectedDifficulty === d ? 'active' : ''}`} onClick={() => setSelectedDifficulty(d)}>{d}</button>
                            ))}
                        </div>
                        <button className="btn btn-generate" onClick={generateProblem} disabled={generatingProblem}>
                            {generatingProblem ? <Loader2 size={14} className="spinner" /> : 'Generate'}
                        </button>
                    </div>
                </div>
            )}

            <div className="twin-challenge-body">
                {/* Left Panel — Problem Description */}
                <div className="twin-left-panel">
                    <div className="panel-header">
                        <h2>{problem?.title || 'Challenge Problem'}</h2>
                        <span className="badge">{problem?.difficulty || 'Easy'}</span>
                    </div>
                    <div className="problem-content">
                        <div className="problem-description">{problem?.description || 'Waiting for problem...'}</div>

                        {/* Dataset Tables */}
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

                        {/* Expected Output */}
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
                    {/* Tab Bar */}
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

                    {/* Editor Area */}
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
        </div>
    );
}

export default TwinChallenge;
