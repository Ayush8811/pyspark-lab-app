import React, { useState, useEffect, useContext } from 'react';
import Editor from '@monaco-editor/react';
import { Play, Loader2, Sparkles, Plus, PanelLeftClose, PanelLeft, ChevronRight, Tags, ArrowLeft, User, LogOut, Bookmark, Trash2 } from 'lucide-react';
import axios from 'axios';
import './App.css';
import { AuthContext } from './AuthContext';
import AuthModal from './AuthModal';
import ProfileDashboard from './ProfileDashboard';
import LandingPage from './LandingPage';
import AILoadingOverlay from './AILoadingOverlay';
import MobileRestrictionOverlay from './MobileRestrictionOverlay';
import { useDeviceType } from './hooks/useDeviceType';
import { API_BASE_URL } from './config';

const TOPICS = [
  // Foundational
  { id: 'select_filter', label: 'Select & Filter', category: 'Foundational' },
  { id: 'sorting', label: 'Sorting & Ordering', category: 'Foundational' },
  { id: 'nulls', label: 'Handling Nulls', category: 'Foundational' },
  { id: 'casting', label: 'Type Casting & Schema', category: 'Foundational' },
  { id: 'duplicates', label: 'Remove Duplicates', category: 'Foundational' },
  { id: 'math', label: 'Basic Math Operations', category: 'Foundational' },

  // Intermediate
  { id: 'agg', label: 'Aggregations & GroupBy', category: 'Intermediate' },
  { id: 'join', label: 'Joins', category: 'Intermediate' },
  { id: 'date', label: 'Date & Time Operations', category: 'Intermediate' },
  { id: 'string', label: 'String Manipulations', category: 'Intermediate' },
  { id: 'pivot', label: 'Pivot & Unpivot', category: 'Intermediate' },
  { id: 'logic', label: 'Conditional Logic (when/otherwise)', category: 'Intermediate' },
  { id: 'unions', label: 'Unions', category: 'Intermediate' },

  // Advanced
  { id: 'window', label: 'Window Functions', category: 'Advanced' },
  { id: 'complex', label: 'Array & Map Operations', category: 'Advanced' },
  { id: 'json', label: 'JSON Parsing', category: 'Advanced' },
  { id: 'udf', label: 'UDFs (User Defined Functions)', category: 'Advanced' },
  { id: 'optimiz', label: 'Performance & Optimization', category: 'Advanced' },
  { id: 'hof', label: 'High-Order Functions', category: 'Advanced' },
];

function App() {
  const { user, token, logout } = useContext(AuthContext);
  const { isMobile } = useDeviceType();
  const [showAuthModal, setShowAuthModal] = useState(false);
  const [showProfileModal, setShowProfileModal] = useState(false);
  const [currentView, setCurrentView] = useState('landing'); // 'landing' | 'ide'

  const [problems, setProblems] = useState([]);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [loading, setLoading] = useState(false);
  const [executing, setExecuting] = useState(false);

  // Split Panel Resize State
  const [leftPanelWidth, setLeftPanelWidth] = useState(40);
  const [editorHeight, setEditorHeight] = useState(60);
  const [isDraggingV, setIsDraggingV] = useState(false);
  const [isDraggingH, setIsDraggingH] = useState(false);

  useEffect(() => {
    const handleMouseMove = (e) => {
      if (isDraggingV) {
        let newWidth = (e.clientX / window.innerWidth) * 100;
        if (newWidth > 20 && newWidth < 80) setLeftPanelWidth(newWidth);
      } else if (isDraggingH) {
        let newHeight = (e.clientY / window.innerHeight) * 100;
        if (newHeight > 20 && newHeight < 80) setEditorHeight(newHeight);
      }
    };

    const handleMouseUp = () => {
      setIsDraggingV(false);
      setIsDraggingH(false);
    };

    if (isDraggingV || isDraggingH) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
      document.body.style.userSelect = 'none'; // Prevent text selection while dragging
    } else {
      document.body.style.userSelect = '';
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
      document.body.style.userSelect = '';
    };
  }, [isDraggingV, isDraggingH]);

  // Sidebar & View states
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [isAddingProblem, setIsAddingProblem] = useState(false);
  const [showingSaved, setShowingSaved] = useState(false); // Library vs Session toggle

  // Add Problem View states (4 Tiers)
  const [selectedDifficulty, setSelectedDifficulty] = useState('Medium');
  const [activeCategory, setActiveCategory] = useState(null); // Level 2
  const [activeTopic, setActiveTopic] = useState(null);       // Level 3 (Hardcoded)
  const [selectedTags, setSelectedTags] = useState([]);       // Level 4 (Dynamic LLM Subtopics)

  const [dynamicSubtopics, setDynamicSubtopics] = useState([]);
  const [loadingSubtopics, setLoadingSubtopics] = useState(false);
  const [loadingMoreSubtopics, setLoadingMoreSubtopics] = useState(false);

  const handleCategoryClick = (category) => {
    setActiveCategory(activeCategory === category ? null : category);
    setActiveTopic(null);
    setDynamicSubtopics([]);
  };

  const handleTopicClick = async (topicObj) => {
    if (activeTopic?.id === topicObj.id) {
      setActiveTopic(null);
      return;
    }

    setActiveTopic(topicObj);

    // Automatically add the Core Topic to selectedTags if not present
    setSelectedTags(prev => {
      if (!prev.find(t => t.topic.id === topicObj.id)) {
        return [...prev, { topic: topicObj, subtopics: [] }];
      }
      return prev;
    });

    setDynamicSubtopics([]);
    setLoadingSubtopics(true);

    try {
      const res = await axios.get(`${API_BASE_URL}/api/topics/subtopics`, {
        // Send the label of the topic to the LLM (e.g. "Window Functions")
        params: { topic: topicObj.label, difficulty: selectedDifficulty }
      });
      setDynamicSubtopics(res.data.subtopics);
    } catch (err) {
      console.error("Failed to generate LLM subtopics", err);
      // fallback handled cleanly empty list instead of messy UI
    } finally {
      setLoadingSubtopics(false);
    }
  };

  const handleGenerateMore = async () => {
    if (!activeTopic) return;
    setLoadingMoreSubtopics(true);
    try {
      const excludeStr = dynamicSubtopics.join(",");
      const res = await axios.get(`${API_BASE_URL}/api/topics/subtopics`, {
        params: { topic: activeTopic.label, difficulty: selectedDifficulty, exclude: excludeStr }
      });
      // Append the new unique subtopics to existing ones
      setDynamicSubtopics(prev => [...prev, ...res.data.subtopics]);
    } catch (err) {
      console.error("Failed to generate more LLM subtopics", err);
    } finally {
      setLoadingMoreSubtopics(false);
    }
  };

  const toggleSubtopic = (subtopicLabel) => {
    if (!activeTopic) return;
    setSelectedTags(prev => {
      return prev.map(item => {
        if (item.topic.id === activeTopic.id) {
          const subs = item.subtopics;
          if (subs.includes(subtopicLabel)) {
            return { ...item, subtopics: subs.filter(s => s !== subtopicLabel) };
          } else {
            return { ...item, subtopics: [...subs, subtopicLabel] };
          }
        }
        return item;
      });
    });
  };

  const removeSubtopic = (topicId, subtopicLabel) => {
    setSelectedTags(prev => prev.map(item => {
      if (item.topic.id === topicId) {
        return { ...item, subtopics: item.subtopics.filter(s => s !== subtopicLabel) };
      }
      return item;
    }));
  };

  const removeCoreTopic = (topicId) => {
    setSelectedTags(prev => prev.filter(item => item.topic.id !== topicId));
    if (activeTopic?.id === topicId) {
      setActiveTopic(null);
    }
  };

  const fetchProblem = async () => {
    // Combine selected tags into a semantic string for the API
    const combinedTopicsList = selectedTags.map(t => {
      if (t.subtopics.length > 0) {
        return `${t.topic.label} (${t.subtopics.join(", ")})`;
      }
      return t.topic.label;
    });

    const combinedTopics = combinedTopicsList.length > 0 ? combinedTopicsList.join("; ") : 'general';

    setLoading(true);

    try {
      const res = await axios.get(`${API_BASE_URL}/api/problem/generate`, {
        params: { topic: combinedTopics, difficulty: selectedDifficulty }
      });

      const newProblem = {
        ...res.data,
        ux_id: Date.now(),
        userCode: res.data.initial_code || '',
        result: null
      };

      setProblems(prev => [...prev, newProblem]);
      setActiveIndex(problems.length);
      setIsAddingProblem(false);
    } catch (err) {
      console.error(err);
      alert('Failed to load problem from backend');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    // Fetch initial problem on load
    if (problems.length === 0) {
      // simulate an initial load from backend
      axios.get(`${API_BASE_URL}/api/problem/generate`, {
        params: { topic: 'Joins', difficulty: 'Easy' }
      }).then(res => {
        setProblems([{
          ...res.data,
          ux_id: Date.now(),
          userCode: res.data.initial_code || '',
          result: null
        }]);
        setActiveIndex(0);
      }).catch(console.error);
    }
  }, []);

  const activeProblem = problems[activeIndex];

  const updateActiveProblem = (updates) => {
    const updatedProblems = [...problems];
    updatedProblems[activeIndex] = { ...activeProblem, ...updates };
    setProblems(updatedProblems);
  };

  const [submitting, setSubmitting] = useState(false);
  const [savingLoading, setSavingLoading] = useState(false);

  const saveProblem = async () => {
    if (!user || !token || !activeProblem) return;
    setSavingLoading(true);
    try {
      // Don't save the ephemeral 'result' and 'userCode' to the DB payload
      const payload = {
        title: activeProblem.title,
        description: activeProblem.description,
        difficulty: activeProblem.difficulty,
        tags: 'User Saved',
        datasets: activeProblem.datasets,
        expected_output: activeProblem.expected_output,
        initial_code: activeProblem.initial_code || activeProblem.userCode
      };

      await axios.post(`${API_BASE_URL}/api/problem/save`, payload, {
        headers: { Authorization: `Bearer ${token}` }
      });
      alert('Problem saved to your library!');
    } catch (err) {
      console.error(err);
      if (err.response?.status === 400) {
        alert('This problem is already in your library!');
      } else {
        alert('Failed to save problem.');
      }
    } finally {
      setSavingLoading(false);
    }
  };

  const fetchSavedProblems = async () => {
    if (!user || !token) return;
    setLoading(true);
    try {
      const res = await axios.get(`${API_BASE_URL}/api/problem/saved`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      // Map saved DB schema to frontend active format
      const mapped = res.data.map((p, idx) => ({
        ...p,
        ux_id: `saved-${Date.now()}-${idx}`,
        userCode: p.initial_code || '',
        id: `DB-${p.id}`,
        result: null
      }));
      setProblems(mapped);
      if (mapped.length > 0) setActiveIndex(0);
      setShowingSaved(true);
      setIsAddingProblem(false);
    } catch (err) {
      console.error(err);
      alert('Failed to fetch saved problems.');
    } finally {
      setLoading(false);
    }
  };

  const deleteSavedProblem = async (problemIdStr) => {
    if (!user || !token) return;
    const dbId = problemIdStr.replace('DB-', '');
    if (!window.confirm("Are you sure you want to remove this saved problem?")) return;
    try {
      await axios.delete(`${API_BASE_URL}/api/problem/saved/${dbId}`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      // Filter out from local state
      const updatedProblems = problems.filter(p => p.id !== problemIdStr);
      setProblems(updatedProblems);
      if (activeProblem?.id === problemIdStr) {
        if (updatedProblems.length > 0) setActiveIndex(0);
      }
    } catch (err) {
      console.error(err);
      alert('Failed to remove problem.');
    }
  };

  const runCode = async () => {
    if (!activeProblem) return;
    setExecuting(true);
    updateActiveProblem({ result: null });

    try {
      const res = await axios.post(`${API_BASE_URL}/api/problem/execute`, {
        code: activeProblem.userCode,
        datasets: activeProblem.datasets || {}
      });
      // Flag as a run execution request
      updateActiveProblem({ result: { ...res.data, type: 'run' } });
    } catch (err) {
      console.error(err);
      updateActiveProblem({
        result: {
          success: false,
          output: '',
          error: err.message,
          type: 'run'
        }
      });
    } finally {
      setExecuting(false);
    }
  };

  const submitCode = async () => {
    if (!activeProblem) return;
    setSubmitting(true);
    updateActiveProblem({ result: null });

    try {
      const res = await axios.post(`${API_BASE_URL}/api/problem/submit`, {
        code: activeProblem.userCode,
        datasets: activeProblem.datasets || {},
        expected_output: activeProblem.expected_output || [],
        difficulty: activeProblem.difficulty || 'Medium'
      }, {
        headers: token ? { Authorization: `Bearer ${token}` } : {}
      });
      // Flag as a generic submit grading result
      updateActiveProblem({ result: { ...res.data, type: 'submit' } });
    } catch (err) {
      console.error(err);
      updateActiveProblem({
        result: {
          success: false,
          passed: false,
          output: '',
          error: err.message,
          type: 'submit',
          message: 'Network error connecting to grader.'
        }
      });
    } finally {
      setSubmitting(false);
    }
  };

  const renderTable = (data) => {
    if (!data || data.length === 0) return <p>No data</p>;
    const keys = Object.keys(data[0]);
    return (
      <table className="dataset-table">
        <thead>
          <tr>{keys.map(k => <th key={k}>{k}</th>)}</tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr key={i}>
              {keys.map(k => <td key={k}>{String(row[k])}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    );
  };



  const categories = Array.from(new Set(TOPICS.map(t => t.category)));

  const getDifficultyColor = (diff) => {
    switch (diff) {
      case 'Easy': return 'var(--success)';
      case 'Medium': return '#f59e0b'; // amber
      case 'Hard': return 'var(--error)';
      default: return 'var(--accent)';
    }
  };

  const handleEditorBeforeMount = (monaco) => {
    monaco.editor.defineTheme('space-theme', {
      base: 'vs-dark',
      inherit: true,
      rules: [
        { background: '0b0014' },
        { token: 'comment', foreground: '64748b', fontStyle: 'italic' },
        { token: 'keyword', foreground: 'a855f7', fontStyle: 'bold' },
        { token: 'number', foreground: 'd8b4fe' }, // Lavender numbers
        { token: 'string', foreground: '3b82f6' }, // Blue strings
      ],
      colors: {
        'editor.background': '#00000000', // Transparent so it blends with panel-bg
        'editor.lineHighlightBackground': '#ffffff0a',
        'editorLineNumber.foreground': '#475569',
        'editorIndentGuide.background': '#ffffff1a',
      }
    });
  };

  if (currentView === 'landing') {
    return (
      <>
        <LandingPage
          onStartPracticing={() => setCurrentView('ide')}
          onShowAuthModal={() => setShowAuthModal(true)}
          onShowProfileModal={() => setShowProfileModal(true)}
          user={user}
          onLogout={logout}
        />
        {showAuthModal && <AuthModal onClose={() => setShowAuthModal(false)} />}
      </>
    );
  }

  if (isMobile) {
    return <MobileRestrictionOverlay onGoBack={() => setCurrentView('landing')} />;
  }

  return (
    <div className="app-container">
      {/* App-Wide Dynamic Space Background */}
      <div className="space-background">
        <div className="glow-orb primary"></div>
        <div className="glow-orb secondary"></div>
      </div>

      {/* Sidebar Panel */}
      <div className={`sidebar ${sidebarOpen ? 'open' : 'closed'}`}>
        <div className="sidebar-header">
          <div
            style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}
            onClick={() => setCurrentView('landing')}
          >
            <Sparkles size={18} style={{ color: 'var(--accent)' }} />
            {sidebarOpen && <span style={{ fontWeight: 600 }}>Practice</span>}
          </div>
          <button className="icon-btn" onClick={() => setSidebarOpen(!sidebarOpen)}>
            {sidebarOpen ? <PanelLeftClose size={18} /> : <PanelLeft size={18} />}
          </button>
        </div>

        {sidebarOpen && (
          <>
            <div className="sidebar-content" style={{ flex: 1, overflowY: 'auto' }}>
              <button
                className="btn btn-full"
                onClick={() => { setIsAddingProblem(true); setShowingSaved(false); }}
                style={{ marginBottom: '1rem' }}
              >
                <Plus size={16} /> Add Problem
              </button>

              {user && (
                <button
                  className="btn btn-full"
                  onClick={() => { showingSaved ? setShowingSaved(false) : fetchSavedProblems() }}
                  style={{
                    marginBottom: '1rem',
                    backgroundColor: showingSaved ? 'rgba(59, 130, 246, 0.2)' : 'transparent',
                    border: '1px solid var(--border)'
                  }}
                >
                  <Bookmark size={16} style={{ color: showingSaved ? 'var(--accent)' : 'inherit' }} />
                  {showingSaved ? 'Return to Session' : 'Saved Problems'}
                </button>
              )}

              <div className="problem-list">
                {problems.map((p, idx) => (
                  <div
                    key={p.ux_id}
                    className={`problem-item ${(!isAddingProblem && idx === activeIndex) ? 'active' : ''}`}
                    onClick={() => {
                      setActiveIndex(idx);
                      setIsAddingProblem(false);
                    }}
                  >
                    <div className="problem-item-details">
                      <span className="problem-item-title">{p.title}</span>
                      <span
                        className="problem-item-id"
                        style={{ color: getDifficultyColor(p.difficulty) }}
                      >
                        {p.id}
                      </span>
                    </div>
                    {showingSaved ? (
                      <button
                        className="icon-btn"
                        onClick={(e) => { e.stopPropagation(); deleteSavedProblem(p.id); }}
                        style={{ padding: '4px', color: 'var(--text-muted)' }}
                        onMouseEnter={e => e.currentTarget.style.color = 'var(--error)'}
                        onMouseLeave={e => e.currentTarget.style.color = 'var(--text-muted)'}
                        title="Remove Problem"
                      >
                        <Trash2 size={14} />
                      </button>
                    ) : (
                      <ChevronRight size={14} className="problem-item-icon" />
                    )}
                  </div>
                ))}
              </div>
            </div>

            {/* User Auth Profile Area */}
            <div className="sidebar-footer">
              {user ? (
                <div className="user-profile">
                  <div
                    style={{ display: 'flex', alignItems: 'center', gap: '10px', cursor: 'pointer' }}
                    onClick={() => setShowProfileModal(true)}
                    title="View Profile Dashboard"
                  >
                    <div className="user-avatar"><User size={20} /></div>
                    <div style={{ display: 'flex', flexDirection: 'column' }}>
                      <span className="username">{user.username}</span>
                      <span className="user-role">Student</span>
                    </div>
                  </div>
                  <button className="icon-btn logout-btn" onClick={logout} title="Logout">
                    <LogOut size={16} />
                  </button>
                </div>
              ) : (
                <button className="btn btn-full" onClick={() => setShowAuthModal(true)} style={{ backgroundColor: 'var(--accent)' }}>
                  Sign In / Register
                </button>
              )}
            </div>
          </>
        )}
      </div>

      {/* Main Content Area */}
      <AILoadingOverlay isVisible={loading} difficulty={selectedDifficulty} />

      <div className="main-content flex-row">

        {isAddingProblem ? (
          /* Redesigned Add Problem Page */
          <div className="add-problem-page">
            <div className="add-problem-header">
              <button className="icon-btn" style={{ marginRight: '1rem' }} onClick={() => setIsAddingProblem(false)}>
                <ArrowLeft size={24} />
              </button>
              <h1>Customize Your Next Challenge</h1>
            </div>

            <div className="add-problem-body">
              {/* Difficulty Selection */}
              <div className="config-section">
                <h3>1. Select Difficulty</h3>
                <div className="segmented-control">
                  {['Easy', 'Medium', 'Hard'].map(diff => (
                    <button
                      key={diff}
                      className={`segment-btn ${selectedDifficulty === diff ? 'active' : ''}`}
                      onClick={() => setSelectedDifficulty(diff)}
                      style={selectedDifficulty === diff ? { borderColor: getDifficultyColor(diff), color: getDifficultyColor(diff) } : {}}
                    >
                      {diff}
                    </button>
                  ))}
                </div>
              </div>

              {/* Tag Routing (Level 2: Category) */}
              <div className="config-section">
                <h3>2. Choose Category</h3>
                <div className="pill-container">
                  {categories.map(cat => (
                    <button
                      key={cat}
                      className={`pill ${activeCategory === cat ? 'active' : ''}`}
                      onClick={() => handleCategoryClick(cat)}
                    >
                      {cat}
                    </button>
                  ))}
                </div>
              </div>

              {/* Tag Selection (Level 3: Hardcoded Topics) */}
              {activeCategory && (
                <div className="config-section fade-in">
                  <h3>3. Select Core Topic</h3>
                  <div className="pill-container">
                    {TOPICS.filter(t => t.category === activeCategory).map(t => {
                      const isSelected = activeTopic?.id === t.id;
                      return (
                        <button
                          key={t.id}
                          className={`pill sub-pill fade-in ${isSelected ? 'selected' : ''}`}
                          onClick={() => handleTopicClick(t)}
                        >
                          {t.label}
                        </button>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* Tag Selection (Level 4: AI Dynamic Subtopics) */}
              {activeTopic && (
                <div className="config-section fade-in">
                  <h3>4. Dynamic AI Specifics {loadingSubtopics && <Loader2 size={14} className="spinner" style={{ display: 'inline', marginLeft: '8px' }} />}</h3>
                  {loadingSubtopics ? (
                    <p style={{ color: 'var(--text-muted)', fontSize: '0.9rem' }}>🧠 AI is actively analyzing the '{activeTopic.label}' topic to generate specific granular practice subtopics...</p>
                  ) : (
                    <div className="pill-container">
                      {dynamicSubtopics.map((t, idx) => {
                        const activeTagObj = selectedTags.find(sel => sel.topic.id === activeTopic.id);
                        const isSelected = activeTagObj ? activeTagObj.subtopics.includes(t) : false;
                        return (
                          <button
                            key={`dyn-${idx}`}
                            className={`pill sub-pill fade-in ${isSelected ? 'selected' : ''}`}
                            onClick={() => toggleSubtopic(t)}
                            style={{ animationDelay: `${idx * 0.05}s`, borderColor: 'var(--accent)' }}
                          >
                            {t}
                            {isSelected && <span style={{ marginLeft: '4px' }}>×</span>}
                          </button>
                        );
                      })}

                      {dynamicSubtopics.length > 0 && (
                        <button
                          className={`pill sub-pill fade-in`}
                          onClick={handleGenerateMore}
                          disabled={loadingMoreSubtopics}
                          style={{ borderColor: 'var(--text-muted)', borderStyle: 'dashed', opacity: 0.8 }}
                          title="Generate more subtopics"
                        >
                          {loadingMoreSubtopics ? <Loader2 size={14} className="spinner" /> : '+ Generate More'}
                        </button>
                      )}
                    </div>
                  )}
                </div>
              )}

              {/* Generate Action Area (Selected Concepts & Action Button) */}
              <div className="generate-action-area" style={{ marginTop: '2rem' }}>

                {/* Selected Concepts Pill Rack */}
                {selectedTags.length > 0 && (
                  <div style={{ marginBottom: '1.5rem', textAlign: 'left', width: '100%' }}>
                    <h4 style={{ marginTop: 0, marginBottom: '0.8rem', color: 'var(--text-main)' }}>Selected Concepts</h4>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.8rem' }}>
                      {selectedTags.map((tagObj) => (
                        <div key={`sel-${tagObj.topic.id}`} style={{
                          display: 'flex',
                          flexDirection: 'column',
                          backgroundColor: 'rgba(255, 255, 255, 0.03)',
                          border: '1px solid var(--border)',
                          borderRadius: '12px',
                          padding: '0.8rem',
                        }}>
                          {/* Core Topic Header */}
                          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: tagObj.subtopics.length > 0 ? '0.5rem' : '0' }}>
                            <span style={{ color: 'var(--accent)', fontWeight: 600, fontSize: '0.95rem' }}>{tagObj.topic.label}</span>
                            <button
                              style={{ background: 'none', border: 'none', color: 'var(--error)', cursor: 'pointer', padding: '2px', fontSize: '1rem' }}
                              onClick={() => removeCoreTopic(tagObj.topic.id)}
                              title="Remove Core Topic"
                            >
                              ×
                            </button>
                          </div>

                          {/* Subtopics row */}
                          {tagObj.subtopics.length > 0 && (
                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.4rem' }}>
                              {tagObj.subtopics.map(sub => (
                                <div key={sub} style={{
                                  display: 'flex', alignItems: 'center',
                                  backgroundColor: 'rgba(59, 130, 246, 0.15)',
                                  borderRadius: '16px', padding: '0.3rem 0.6rem', fontSize: '0.85rem',
                                  border: '1px solid rgba(59, 130, 246, 0.3)'
                                }}>
                                  <span style={{ color: 'var(--text-main)', marginRight: '6px' }}>{sub}</span>
                                  <button
                                    style={{ background: 'none', border: 'none', color: 'var(--text-muted)', cursor: 'pointer', display: 'flex', padding: '2px' }}
                                    onClick={() => removeSubtopic(tagObj.topic.id, sub)}
                                  >
                                    ×
                                  </button>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                <button
                  className="btn btn-generate"
                  onClick={fetchProblem}
                  disabled={loading || selectedTags.length === 0}
                  style={{
                    backgroundColor: getDifficultyColor(selectedDifficulty),
                    opacity: loading || selectedTags.length === 0 ? 0.6 : 1,
                    cursor: loading || selectedTags.length === 0 ? 'not-allowed' : 'pointer'
                  }}
                >
                  <Sparkles size={18} /> Generate {selectedDifficulty} Challenge
                </button>
              </div>

            </div>
          </div>
        ) : (
          /* Split Screen IDE View */
          <>
            {/* Left Panel - Problem Description */}
            <div className="left-panel" style={{ width: `${leftPanelWidth}%` }}>
              <div className="panel-header">
                <h2>Problem Description</h2>
              </div>

              <div className="problem-content">
                {activeProblem ? (
                  <>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '1rem' }}>
                      <h1 style={{ fontSize: '1.5rem', margin: 0 }}>{activeProblem.title}</h1>
                      <div className="badge-group" style={{ display: 'flex', gap: '0.8rem', alignItems: 'center' }}>

                        {user && !showingSaved && (
                          <button
                            className="btn icon-btn"
                            style={{ padding: '0.3rem 0.6rem', fontSize: '0.85rem', display: 'flex', alignItems: 'center', gap: '0.3rem' }}
                            onClick={saveProblem}
                            disabled={savingLoading}
                          >
                            <Bookmark size={14} /> {savingLoading ? 'Saving...' : 'Save Problem'}
                          </button>
                        )}

                        <span style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>{activeProblem.id}</span>
                        <div className="badge" style={{ borderColor: getDifficultyColor(activeProblem.difficulty), color: getDifficultyColor(activeProblem.difficulty), backgroundColor: 'transparent', border: '1px solid' }}>
                          {activeProblem.difficulty}
                        </div>
                      </div>
                    </div>

                    <div className="problem-description">
                      {activeProblem.description}
                    </div>

                    <h3>Datasets</h3>
                    {Object.entries(activeProblem.datasets || {}).map(([name, data]) => (
                      <div key={name}>
                        <div className="dataset-title">Table: {name}</div>
                        {renderTable(data)}
                      </div>
                    ))}

                    <h3 style={{ marginTop: '2rem' }}>Expected Output</h3>
                    <div style={{ opacity: 0.8 }}>
                      {renderTable(activeProblem.expected_output)}
                    </div>
                  </>
                ) : (
                  <div style={{ display: 'flex', justifyContent: 'center', marginTop: '3rem' }}>
                    <Loader2 size={32} className="spinner" style={{ color: 'var(--accent)' }} />
                  </div>
                )}
              </div>
            </div>

            {/* Vertical Resizer */}
            <div
              className="vertical-resizer"
              onMouseDown={() => setIsDraggingV(true)}
            />

            {/* Right Panel - Code & Results */}
            <div className="right-panel" style={{ width: `${100 - leftPanelWidth}%`, position: 'relative' }}>

              {/* Grading Overlay (Phase 5) */}
              {activeProblem?.result?.type === 'submit' && (
                <div className="grading-overlay" onClick={() => updateActiveProblem({ result: null })}>
                  <div className="grading-content" onClick={e => e.stopPropagation()}>
                    {activeProblem.result.passed ? (
                      <>
                        <div className="grading-emoji">🎉</div>
                        <h2 className="grading-message">Congratulations!</h2>
                        <p className="grading-details" style={{ color: 'var(--success)' }}>All test cases passed! Your PySpark logic perfectly matched the expected output.</p>
                      </>
                    ) : (
                      <>
                        <div className="grading-emoji">😔</div>
                        <h2 className="grading-message">Not quite right! Try Again.</h2>
                        <p className="grading-details" style={{ color: 'var(--error)' }}>{activeProblem.result.message}</p>
                      </>
                    )}
                    <button className="btn btn-submit" onClick={() => updateActiveProblem({ result: null })} style={{ margin: '0 auto' }}>
                      Continue
                    </button>
                  </div>
                </div>
              )}

              <div className="editor-container" style={{ flex: 'none', height: `${editorHeight}%` }}>
                <div className="panel-header">
                  <h2>main.py</h2>
                  <div style={{ display: 'flex', gap: '10px' }}>
                    <button className="btn btn-success" onClick={runCode} disabled={executing || submitting || !activeProblem}>
                      {executing ? <Loader2 size={16} className="spinner" /> : <><Play size={16} /> Run Code</>}
                    </button>
                    <button className="btn btn-submit" onClick={submitCode} disabled={executing || submitting || !activeProblem}>
                      {submitting ? <Loader2 size={16} className="spinner" /> : 'Submit Code'}
                    </button>
                  </div>
                </div>
                <Editor
                  height="calc(100% - 60px)"
                  defaultLanguage="python"
                  theme="space-theme"
                  beforeMount={handleEditorBeforeMount}
                  value={activeProblem?.userCode || ''}
                  onChange={(val) => updateActiveProblem({ userCode: val })}
                  options={{
                    minimap: { enabled: false },
                    fontSize: 14,
                    fontFamily: "'Consolas', 'Courier New', monospace",
                    scrollBeyondLastLine: false,
                  }}
                />
              </div>

              {/* Horizontal Resizer */}
              <div
                className="horizontal-resizer"
                onMouseDown={() => setIsDraggingH(true)}
              />

              <div className="results-container" style={{ flex: 'none', height: `${100 - editorHeight}%` }}>
                <div className="panel-header">
                  <h2>Execution Results</h2>
                  {activeProblem?.result && (
                    <span className={activeProblem.result.success ? "success-text" : "error-text"}>
                      {activeProblem.result.success
                        ? (activeProblem.result.type === 'run' ? "Success ✓✓" : "Success")
                        : "Error"}
                    </span>
                  )}
                </div>
                <div className="results-content">
                  {!activeProblem?.result ? (
                    <span style={{ color: 'var(--text-muted)' }}>Run your code to see results here...</span>
                  ) : (
                    <>
                      {activeProblem.result.output && (
                        <pre className="output-pre">{activeProblem.result.output}</pre>
                      )}
                      {activeProblem.result.error && (
                        <pre className="error-text" style={{ marginTop: '1rem', whiteSpace: 'pre-wrap' }}>{activeProblem.result.error}</pre>
                      )}
                    </>
                  )}
                </div>
              </div>
            </div>
          </>
        )}
      </div>

      {showAuthModal && <AuthModal onClose={() => setShowAuthModal(false)} />}
      {showProfileModal && <ProfileDashboard onClose={() => setShowProfileModal(false)} />}
    </div>
  );
}

export default App;
