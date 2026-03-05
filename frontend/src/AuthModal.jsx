import React, { useState, useContext } from 'react';
import axios from 'axios';
import { AuthContext } from './AuthContext';
import { API_BASE_URL } from './config';
import './AuthModal.css';

const AuthModal = ({ onClose }) => {
    const [isLogin, setIsLogin] = useState(true);
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [error, setError] = useState('');
    const [loading, setLoading] = useState(false);

    const { login } = useContext(AuthContext);

    const handleSubmit = async (e) => {
        e.preventDefault();
        setError('');
        setLoading(true);

        try {
            if (isLogin) {
                // Login Request (OAuth2 form data format)
                const formData = new URLSearchParams();
                formData.append('username', username);
                formData.append('password', password);

                // The actual login API call
                const res = await axios.post(`${API_BASE_URL}/api/auth/login`, formData, {
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
                });

                // Use the context's login function to set the token
                login(res.data.access_token);
                onClose(); // Just close the modal, let App.jsx keep them on landing page
            } else {
                // Register Request (JSON format)
                await axios.post(`${API_BASE_URL}/api/auth/register`, {
                    username,
                    password
                });

                // Auto login after register
                const loginRes = await axios.post(`${API_BASE_URL}/api/auth/login`, formData, {
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
                });
                login(loginRes.data.access_token);
                onClose();
            }
        } catch (err) {
            setError(err.response?.data?.detail || 'Authentication failed. Please try again.');
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="auth-modal-overlay" onClick={onClose}>
            <div className="auth-modal-content" onClick={e => e.stopPropagation()}>
                <button className="close-btn" onClick={onClose}>&times;</button>
                <h2>{isLogin ? 'Welcome Back' : 'Create an Account'}</h2>

                {error && <div className="auth-error">{error}</div>}

                <form onSubmit={handleSubmit} className="auth-form">
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
                    <div className="form-group">
                        <label>Password</label>
                        <input
                            type="password"
                            value={password}
                            onChange={e => setPassword(e.target.value)}
                            required
                        />
                    </div>
                    <button type="submit" className="auth-submit-btn" disabled={loading}>
                        {loading ? 'Processing...' : (isLogin ? 'Login' : 'Register')}
                    </button>
                </form>

                <div className="auth-toggle">
                    {isLogin ? "Don't have an account? " : "Already have an account? "}
                    <span onClick={() => setIsLogin(!isLogin)}>
                        {isLogin ? 'Register here' : 'Login here'}
                    </span>
                </div>
            </div>
        </div>
    );
};

export default AuthModal;
