import React, { useState, useContext } from 'react';
import axios from 'axios';
import { AuthContext } from './AuthContext';
import { API_BASE_URL } from './config';
import './AuthModal.css';

const AuthModal = ({ onClose }) => {
    // viewMode can be 'LOGIN', 'REGISTER', 'FORGOT', 'RESET'
    const [viewMode, setViewMode] = useState('LOGIN');
    const [username, setUsername] = useState('');
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const [resetCode, setResetCode] = useState('');
    const [error, setError] = useState('');
    const [successMsg, setSuccessMsg] = useState('');
    const [loading, setLoading] = useState(false);

    const { login } = useContext(AuthContext);

    const handleSubmit = async (e) => {
        e.preventDefault();
        setError('');
        setSuccessMsg('');
        setLoading(true);

        try {
            if (viewMode === 'LOGIN') {
                // Login Request
                const formData = new URLSearchParams();
                formData.append('username', username);
                formData.append('password', password);

                const res = await axios.post(`${API_BASE_URL}/api/auth/login`, formData, {
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
                });

                login(res.data.access_token);
                onClose();

            } else if (viewMode === 'REGISTER') {
                // Register Request
                await axios.post(`${API_BASE_URL}/api/auth/register`, {
                    username,
                    email: email || null,
                    password
                });

                // Auto login after register
                const formData = new URLSearchParams();
                formData.append('username', username);
                formData.append('password', password);

                const loginRes = await axios.post(`${API_BASE_URL}/api/auth/login`, formData, {
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
                });
                login(loginRes.data.access_token);
                onClose();

            } else if (viewMode === 'FORGOT') {
                // Forgot Password Request
                const res = await axios.post(`${API_BASE_URL}/api/auth/forgot-password`, {
                    email
                });
                setSuccessMsg(res.data.message);
                setViewMode('RESET');

            } else if (viewMode === 'RESET') {
                // Reset Password Request
                const res = await axios.post(`${API_BASE_URL}/api/auth/reset-password`, {
                    email,
                    code: resetCode,
                    new_password: password
                });
                setSuccessMsg(res.data.message);
                setViewMode('LOGIN');
            }
        } catch (err) {
            setError(err.response?.data?.detail || 'Authentication failed. Please try again.');
        } finally {
            setLoading(false);
        }
    };

    const getTitle = () => {
        if (viewMode === 'LOGIN') return 'Welcome Back';
        if (viewMode === 'REGISTER') return 'Create an Account';
        if (viewMode === 'FORGOT') return 'Reset Password';
        if (viewMode === 'RESET') return 'Enter Recovery Code';
    };

    const getSubmitText = () => {
        if (loading) return 'Processing...';
        if (viewMode === 'LOGIN') return 'Login';
        if (viewMode === 'REGISTER') return 'Register';
        if (viewMode === 'FORGOT') return 'Send Recovery Code';
        if (viewMode === 'RESET') return 'Set New Password';
    };

    return (
        <div className="auth-modal-overlay" onClick={onClose}>
            <div className="auth-modal-content" onClick={e => e.stopPropagation()}>
                <button className="close-btn" onClick={onClose}>&times;</button>
                <h2>{getTitle()}</h2>

                {error && <div className="auth-error">{error}</div>}
                {successMsg && <div className="auth-success" style={{ color: 'var(--eco-green)', marginBottom: '15px', padding: '10px', backgroundColor: 'rgba(57, 255, 20, 0.1)', borderRadius: '6px', fontSize: '0.9rem' }}>{successMsg}</div>}

                <form onSubmit={handleSubmit} className="auth-form">
                    {(viewMode === 'LOGIN' || viewMode === 'REGISTER') && (
                        <div className="form-group">
                            <label>Username</label>
                            <input
                                type="text"
                                value={username}
                                onChange={e => setUsername(e.target.value)}
                                required
                                autoFocus
                            />
                        </div>
                    )}

                    {(viewMode === 'REGISTER' || viewMode === 'FORGOT' || viewMode === 'RESET') && (
                        <div className="form-group">
                            <label>Email Address</label>
                            <input
                                type="email"
                                value={email}
                                onChange={e => setEmail(e.target.value)}
                                required
                                autoFocus={viewMode === 'FORGOT' || viewMode === 'RESET'}
                            />
                        </div>
                    )}

                    {viewMode === 'RESET' && (
                        <div className="form-group">
                            <label>6-Digit Recovery Code</label>
                            <input
                                type="text"
                                value={resetCode}
                                onChange={e => setResetCode(e.target.value)}
                                required
                                placeholder="e.g. 123456"
                                maxLength={6}
                            />
                        </div>
                    )}

                    {(viewMode === 'LOGIN' || viewMode === 'REGISTER' || viewMode === 'RESET') && (
                        <div className="form-group">
                            <label>{viewMode === 'RESET' ? 'New Password' : 'Password'}</label>
                            <input
                                type="password"
                                value={password}
                                onChange={e => setPassword(e.target.value)}
                                required
                            />
                        </div>
                    )}

                    {viewMode === 'LOGIN' && (
                        <div className="forgot-password-link" style={{ textAlign: 'right', marginTop: '-10px', marginBottom: '15px' }}>
                            <span
                                onClick={() => { setViewMode('FORGOT'); setError(''); setSuccessMsg(''); }}
                                style={{ color: 'var(--cyber-blue)', fontSize: '0.8rem', cursor: 'pointer', textDecoration: 'underline' }}>
                                Forgot Password?
                            </span>
                        </div>
                    )}

                    <button type="submit" className="auth-submit-btn" disabled={loading}>
                        {getSubmitText()}
                    </button>
                </form>

                <div className="auth-toggle">
                    {viewMode === 'LOGIN' && (
                        <>Don't have an account? <span onClick={() => { setViewMode('REGISTER'); setError(''); setSuccessMsg(''); }}>Register here</span></>
                    )}
                    {(viewMode === 'REGISTER' || viewMode === 'FORGOT' || viewMode === 'RESET') && (
                        <>Back to <span onClick={() => { setViewMode('LOGIN'); setError(''); setSuccessMsg(''); }}>Login</span></>
                    )}
                </div>
            </div>
        </div>
    );
};

export default AuthModal;
