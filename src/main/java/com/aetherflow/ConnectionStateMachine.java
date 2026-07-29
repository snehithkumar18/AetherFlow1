package com.aetherflow;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;





public class ConnectionStateMachine {
    
    
    public enum ConnectionState {
        
        IDLE(0),
        INITIALIZING(1),
        RESOLVING(2),
        CONNECTING(3),
        
        
        HANDSHAKE_INITIATED(4),
        HANDSHAKE_SENT(5),
        HANDSHAKE_RECEIVED(6),
        HANDSHAKE_VALIDATING(7),
        HANDSHAKE_COMPLETE(8),
        
        
        AUTH_INITIATED(9),
        AUTH_CHALLENGE_SENT(10),
        AUTH_CHALLENGE_RECEIVED(11),
        AUTH_RESPONSE_SENT(12),
        AUTH_RESPONSE_RECEIVED(13),
        AUTH_VALIDATING(14),
        AUTH_COMPLETE(15),
        
        
        SESSION_CREATING(16),
        SESSION_ACTIVE(17),
        SESSION_REFRESHING(18),
        
        
        DATA_TRANSFER_READY(19),
        DATA_TRANSFER_ACTIVE(20),
        DATA_TRANSFER_PAUSED(21),
        
        
        KEY_ROTATION_INITIATED(22),
        KEY_ROTATION_IN_PROGRESS(23),
        KEY_ROTATION_COMPLETE(24),
        
        
        FRAGMENTATION_ACTIVE(25),
        REASSEMBLY_ACTIVE(26),
        
        
        FLOW_CONTROL_BLOCKED(27),
        FLOW_CONTROL_RECOVERING(28),
        
        
        ERROR_DETECTED(29),
        ERROR_RECOVERING(30),
        CLOSING(31),
        CLOSED(32),
        TERMINATED(33);
        
        private final int stateId;
        
        ConnectionState(int stateId) {
            this.stateId = stateId;
        }
        
        public int getStateId() {
            return stateId;
        }
        
        public static ConnectionState fromId(int id) {
            for (ConnectionState state : values()) {
                if (state.stateId == id) {
                    return state;
                }
            }
            return null;
        }
    }
    
    
    public enum StateEvent {
        CONNECT_REQUEST,
        CONNECT_SUCCESS,
        CONNECT_FAILED,
        HANDSHAKE_INITIATE,
        HANDSHAKE_SENT,
        HANDSHAKE_RECEIVED,
        HANDSHAKE_VALIDATED,
        HANDSHAKE_FAILED,
        AUTH_INITIATE,
        AUTH_CHALLENGE,
        AUTH_RESPONSE,
        AUTH_VALIDATED,
        AUTH_FAILED,
        SESSION_CREATE,
        SESSION_REFRESH,
        SESSION_EXPIRED,
        SESSION_ACTIVE,
        DATA_SEND,
        DATA_RECEIVE,
        DATA_PAUSE,
        DATA_RESUME,
        KEY_ROTATION_INITIATE,
        KEY_ROTATION_COMPLETE,
        FRAGMENTATION_START,
        FRAGMENTATION_COMPLETE,
        REASSEMBLY_START,
        REASSEMBLY_COMPLETE,
        FLOW_CONTROL_BLOCK,
        FLOW_CONTROL_UNBLOCK,
        ERROR_DETECTED,
        ERROR_RECOVERED,
        CLOSE_REQUEST,
        CLOSE_COMPLETE,
        TIMEOUT,
        RESET
    }
    
    
    private static final Map<ConnectionState, Map<StateEvent, ConnectionState>> TRANSITION_TABLE;
    
    static {
        TRANSITION_TABLE = new HashMap<>();
        
        
        Map<StateEvent, ConnectionState> idleTransitions = new HashMap<>();
        idleTransitions.put(StateEvent.CONNECT_REQUEST, ConnectionState.INITIALIZING);
        idleTransitions.put(StateEvent.RESET, ConnectionState.IDLE);
        TRANSITION_TABLE.put(ConnectionState.IDLE, idleTransitions);
        
        
        Map<StateEvent, ConnectionState> initTransitions = new HashMap<>();
        initTransitions.put(StateEvent.CONNECT_SUCCESS, ConnectionState.RESOLVING);
        initTransitions.put(StateEvent.CONNECT_FAILED, ConnectionState.ERROR_DETECTED);
        initTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.INITIALIZING, initTransitions);
        
        
        Map<StateEvent, ConnectionState> resolvingTransitions = new HashMap<>();
        resolvingTransitions.put(StateEvent.CONNECT_SUCCESS, ConnectionState.CONNECTING);
        resolvingTransitions.put(StateEvent.CONNECT_FAILED, ConnectionState.ERROR_DETECTED);
        resolvingTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.RESOLVING, resolvingTransitions);
        
        
        Map<StateEvent, ConnectionState> connectingTransitions = new HashMap<>();
        connectingTransitions.put(StateEvent.CONNECT_SUCCESS, ConnectionState.HANDSHAKE_INITIATED);
        connectingTransitions.put(StateEvent.CONNECT_FAILED, ConnectionState.ERROR_DETECTED);
        connectingTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.CONNECTING, connectingTransitions);
        
        
        Map<StateEvent, ConnectionState> handshakeInitTransitions = new HashMap<>();
        handshakeInitTransitions.put(StateEvent.HANDSHAKE_SENT, ConnectionState.HANDSHAKE_SENT);
        handshakeInitTransitions.put(StateEvent.HANDSHAKE_RECEIVED, ConnectionState.HANDSHAKE_RECEIVED);
        handshakeInitTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.HANDSHAKE_INITIATED, handshakeInitTransitions);
        
        
        Map<StateEvent, ConnectionState> handshakeSentTransitions = new HashMap<>();
        handshakeSentTransitions.put(StateEvent.HANDSHAKE_RECEIVED, ConnectionState.HANDSHAKE_VALIDATING);
        handshakeSentTransitions.put(StateEvent.HANDSHAKE_FAILED, ConnectionState.ERROR_DETECTED);
        handshakeSentTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.HANDSHAKE_SENT, handshakeSentTransitions);
        
        
        Map<StateEvent, ConnectionState> handshakeReceivedTransitions = new HashMap<>();
        handshakeReceivedTransitions.put(StateEvent.HANDSHAKE_VALIDATED, ConnectionState.HANDSHAKE_COMPLETE);
        handshakeReceivedTransitions.put(StateEvent.HANDSHAKE_FAILED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.HANDSHAKE_RECEIVED, handshakeReceivedTransitions);
        
        
        Map<StateEvent, ConnectionState> handshakeValidatingTransitions = new HashMap<>();
        handshakeValidatingTransitions.put(StateEvent.HANDSHAKE_VALIDATED, ConnectionState.HANDSHAKE_COMPLETE);
        handshakeValidatingTransitions.put(StateEvent.HANDSHAKE_FAILED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.HANDSHAKE_VALIDATING, handshakeValidatingTransitions);
        
        
        Map<StateEvent, ConnectionState> handshakeCompleteTransitions = new HashMap<>();
        handshakeCompleteTransitions.put(StateEvent.AUTH_INITIATE, ConnectionState.AUTH_INITIATED);
        handshakeCompleteTransitions.put(StateEvent.CLOSE_REQUEST, ConnectionState.CLOSING);
        TRANSITION_TABLE.put(ConnectionState.HANDSHAKE_COMPLETE, handshakeCompleteTransitions);
        
        
        Map<StateEvent, ConnectionState> authInitTransitions = new HashMap<>();
        authInitTransitions.put(StateEvent.AUTH_CHALLENGE, ConnectionState.AUTH_CHALLENGE_SENT);
        authInitTransitions.put(StateEvent.AUTH_CHALLENGE, ConnectionState.AUTH_CHALLENGE_RECEIVED);
        authInitTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.AUTH_INITIATED, authInitTransitions);
        
        
        Map<StateEvent, ConnectionState> authChallengeSentTransitions = new HashMap<>();
        authChallengeSentTransitions.put(StateEvent.AUTH_RESPONSE, ConnectionState.AUTH_RESPONSE_RECEIVED);
        authChallengeSentTransitions.put(StateEvent.AUTH_FAILED, ConnectionState.ERROR_DETECTED);
        authChallengeSentTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.AUTH_CHALLENGE_SENT, authChallengeSentTransitions);
        
        
        Map<StateEvent, ConnectionState> authChallengeReceivedTransitions = new HashMap<>();
        authChallengeReceivedTransitions.put(StateEvent.AUTH_RESPONSE, ConnectionState.AUTH_RESPONSE_SENT);
        authChallengeReceivedTransitions.put(StateEvent.AUTH_FAILED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.AUTH_CHALLENGE_RECEIVED, authChallengeReceivedTransitions);
        
        
        Map<StateEvent, ConnectionState> authResponseSentTransitions = new HashMap<>();
        authResponseSentTransitions.put(StateEvent.AUTH_VALIDATED, ConnectionState.AUTH_VALIDATING);
        authResponseSentTransitions.put(StateEvent.AUTH_FAILED, ConnectionState.ERROR_DETECTED);
        authResponseSentTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.AUTH_RESPONSE_SENT, authResponseSentTransitions);
        
        
        Map<StateEvent, ConnectionState> authResponseReceivedTransitions = new HashMap<>();
        authResponseReceivedTransitions.put(StateEvent.AUTH_VALIDATED, ConnectionState.AUTH_VALIDATING);
        authResponseReceivedTransitions.put(StateEvent.AUTH_FAILED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.AUTH_RESPONSE_RECEIVED, authResponseReceivedTransitions);
        
        
        Map<StateEvent, ConnectionState> authValidatingTransitions = new HashMap<>();
        authValidatingTransitions.put(StateEvent.AUTH_VALIDATED, ConnectionState.AUTH_COMPLETE);
        authValidatingTransitions.put(StateEvent.AUTH_FAILED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.AUTH_VALIDATING, authValidatingTransitions);
        
        
        Map<StateEvent, ConnectionState> authCompleteTransitions = new HashMap<>();
        authCompleteTransitions.put(StateEvent.SESSION_CREATE, ConnectionState.SESSION_CREATING);
        authCompleteTransitions.put(StateEvent.CLOSE_REQUEST, ConnectionState.CLOSING);
        TRANSITION_TABLE.put(ConnectionState.AUTH_COMPLETE, authCompleteTransitions);
        
        
        Map<StateEvent, ConnectionState> sessionCreatingTransitions = new HashMap<>();
        sessionCreatingTransitions.put(StateEvent.SESSION_CREATE, ConnectionState.SESSION_ACTIVE);
        sessionCreatingTransitions.put(StateEvent.SESSION_ACTIVE, ConnectionState.SESSION_ACTIVE);
        sessionCreatingTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.SESSION_CREATING, sessionCreatingTransitions);
        
        
        Map<StateEvent, ConnectionState> sessionActiveTransitions = new HashMap<>();
        sessionActiveTransitions.put(StateEvent.DATA_SEND, ConnectionState.DATA_TRANSFER_ACTIVE);
        sessionActiveTransitions.put(StateEvent.DATA_RECEIVE, ConnectionState.DATA_TRANSFER_ACTIVE);
        sessionActiveTransitions.put(StateEvent.SESSION_REFRESH, ConnectionState.SESSION_REFRESHING);
        sessionActiveTransitions.put(StateEvent.SESSION_EXPIRED, ConnectionState.ERROR_DETECTED);
        sessionActiveTransitions.put(StateEvent.KEY_ROTATION_INITIATE, ConnectionState.KEY_ROTATION_INITIATED);
        sessionActiveTransitions.put(StateEvent.FRAGMENTATION_START, ConnectionState.FRAGMENTATION_ACTIVE);
        sessionActiveTransitions.put(StateEvent.FLOW_CONTROL_BLOCK, ConnectionState.FLOW_CONTROL_BLOCKED);
        sessionActiveTransitions.put(StateEvent.CLOSE_REQUEST, ConnectionState.CLOSING);
        TRANSITION_TABLE.put(ConnectionState.SESSION_ACTIVE, sessionActiveTransitions);
        
        
        Map<StateEvent, ConnectionState> sessionRefreshingTransitions = new HashMap<>();
        sessionRefreshingTransitions.put(StateEvent.SESSION_CREATE, ConnectionState.SESSION_ACTIVE);
        sessionRefreshingTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.SESSION_REFRESHING, sessionRefreshingTransitions);
        
        
        Map<StateEvent, ConnectionState> dataTransferActiveTransitions = new HashMap<>();
        dataTransferActiveTransitions.put(StateEvent.DATA_PAUSE, ConnectionState.DATA_TRANSFER_PAUSED);
        dataTransferActiveTransitions.put(StateEvent.FLOW_CONTROL_BLOCK, ConnectionState.FLOW_CONTROL_BLOCKED);
        dataTransferActiveTransitions.put(StateEvent.FRAGMENTATION_START, ConnectionState.FRAGMENTATION_ACTIVE);
        dataTransferActiveTransitions.put(StateEvent.REASSEMBLY_START, ConnectionState.REASSEMBLY_ACTIVE);
        dataTransferActiveTransitions.put(StateEvent.CLOSE_REQUEST, ConnectionState.CLOSING);
        dataTransferActiveTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.DATA_TRANSFER_ACTIVE, dataTransferActiveTransitions);
        
        
        Map<StateEvent, ConnectionState> dataTransferPausedTransitions = new HashMap<>();
        dataTransferPausedTransitions.put(StateEvent.DATA_RESUME, ConnectionState.DATA_TRANSFER_ACTIVE);
        dataTransferPausedTransitions.put(StateEvent.CLOSE_REQUEST, ConnectionState.CLOSING);
        TRANSITION_TABLE.put(ConnectionState.DATA_TRANSFER_PAUSED, dataTransferPausedTransitions);
        
        
        Map<StateEvent, ConnectionState> keyRotationInitTransitions = new HashMap<>();
        keyRotationInitTransitions.put(StateEvent.KEY_ROTATION_COMPLETE, ConnectionState.KEY_ROTATION_COMPLETE);
        keyRotationInitTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        keyRotationInitTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.KEY_ROTATION_INITIATED, keyRotationInitTransitions);
        
        
        Map<StateEvent, ConnectionState> keyRotationInProgressTransitions = new HashMap<>();
        keyRotationInProgressTransitions.put(StateEvent.KEY_ROTATION_COMPLETE, ConnectionState.KEY_ROTATION_COMPLETE);
        keyRotationInProgressTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.KEY_ROTATION_IN_PROGRESS, keyRotationInProgressTransitions);
        
        
        Map<StateEvent, ConnectionState> keyRotationCompleteTransitions = new HashMap<>();
        keyRotationCompleteTransitions.put(StateEvent.SESSION_ACTIVE, ConnectionState.SESSION_ACTIVE);
        TRANSITION_TABLE.put(ConnectionState.KEY_ROTATION_COMPLETE, keyRotationCompleteTransitions);
        
        
        Map<StateEvent, ConnectionState> fragmentationActiveTransitions = new HashMap<>();
        fragmentationActiveTransitions.put(StateEvent.FRAGMENTATION_COMPLETE, ConnectionState.DATA_TRANSFER_ACTIVE);
        fragmentationActiveTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.FRAGMENTATION_ACTIVE, fragmentationActiveTransitions);
        
        
        Map<StateEvent, ConnectionState> reassemblyActiveTransitions = new HashMap<>();
        reassemblyActiveTransitions.put(StateEvent.REASSEMBLY_COMPLETE, ConnectionState.DATA_TRANSFER_ACTIVE);
        reassemblyActiveTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.REASSEMBLY_ACTIVE, reassemblyActiveTransitions);
        
        
        Map<StateEvent, ConnectionState> flowControlBlockedTransitions = new HashMap<>();
        flowControlBlockedTransitions.put(StateEvent.FLOW_CONTROL_UNBLOCK, ConnectionState.FLOW_CONTROL_RECOVERING);
        flowControlBlockedTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.FLOW_CONTROL_BLOCKED, flowControlBlockedTransitions);
        
        
        Map<StateEvent, ConnectionState> flowControlRecoveringTransitions = new HashMap<>();
        flowControlRecoveringTransitions.put(StateEvent.DATA_SEND, ConnectionState.DATA_TRANSFER_ACTIVE);
        flowControlRecoveringTransitions.put(StateEvent.DATA_RECEIVE, ConnectionState.DATA_TRANSFER_ACTIVE);
        TRANSITION_TABLE.put(ConnectionState.FLOW_CONTROL_RECOVERING, flowControlRecoveringTransitions);
        
        
        Map<StateEvent, ConnectionState> errorDetectedTransitions = new HashMap<>();
        errorDetectedTransitions.put(StateEvent.ERROR_RECOVERED, ConnectionState.ERROR_RECOVERING);
        errorDetectedTransitions.put(StateEvent.CLOSE_REQUEST, ConnectionState.CLOSING);
        errorDetectedTransitions.put(StateEvent.RESET, ConnectionState.IDLE);
        TRANSITION_TABLE.put(ConnectionState.ERROR_DETECTED, errorDetectedTransitions);
        
        
        Map<StateEvent, ConnectionState> errorRecoveringTransitions = new HashMap<>();
        errorRecoveringTransitions.put(StateEvent.SESSION_ACTIVE, ConnectionState.SESSION_ACTIVE);
        errorRecoveringTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        errorRecoveringTransitions.put(StateEvent.CLOSE_REQUEST, ConnectionState.CLOSING);
        TRANSITION_TABLE.put(ConnectionState.ERROR_RECOVERING, errorRecoveringTransitions);
        
        
        Map<StateEvent, ConnectionState> closingTransitions = new HashMap<>();
        closingTransitions.put(StateEvent.CLOSE_COMPLETE, ConnectionState.CLOSED);
        closingTransitions.put(StateEvent.TIMEOUT, ConnectionState.TERMINATED);
        TRANSITION_TABLE.put(ConnectionState.CLOSING, closingTransitions);
        
        
        Map<StateEvent, ConnectionState> closedTransitions = new HashMap<>();
        closedTransitions.put(StateEvent.CONNECT_REQUEST, ConnectionState.INITIALIZING);
        closedTransitions.put(StateEvent.RESET, ConnectionState.IDLE);
        TRANSITION_TABLE.put(ConnectionState.CLOSED, closedTransitions);
        
        
        Map<StateEvent, ConnectionState> terminatedTransitions = new HashMap<>();
        terminatedTransitions.put(StateEvent.RESET, ConnectionState.IDLE);
        TRANSITION_TABLE.put(ConnectionState.TERMINATED, terminatedTransitions);
    }
    
    
    private volatile ConnectionState currentState;
    
    
    private final int connectionId;
    
    
    private final java.util.List<StateTransition> transitionHistory;
    
    
    private final Map<ConnectionState, AtomicInteger> stateVisitCounters;
    
    
    private final ReentrantLock stateLock;
    
    
    private final java.util.List<StateChangeListener> listeners;
    
    
    private static final int MAX_TRANSITION_HISTORY = 1000;
    
    
    private final Map<ConnectionState, Integer> stateTimeouts;
    
    


    public ConnectionStateMachine(int connectionId) {
        this.connectionId = connectionId;
        this.currentState = ConnectionState.IDLE;
        this.transitionHistory = new java.util.ArrayList<>();
        this.stateVisitCounters = new ConcurrentHashMap<>();
        this.stateLock = new ReentrantLock();
        this.listeners = new java.util.ArrayList<>();
        this.stateTimeouts = new HashMap<>();
        
        
        for (ConnectionState state : ConnectionState.values()) {
            stateVisitCounters.put(state, new AtomicInteger(0));
        }
        
        
        stateTimeouts.put(ConnectionState.INITIALIZING, 30);
        stateTimeouts.put(ConnectionState.RESOLVING, 30);
        stateTimeouts.put(ConnectionState.CONNECTING, 60);
        stateTimeouts.put(ConnectionState.HANDSHAKE_INITIATED, 30);
        stateTimeouts.put(ConnectionState.HANDSHAKE_SENT, 30);
        stateTimeouts.put(ConnectionState.HANDSHAKE_VALIDATING, 30);
        stateTimeouts.put(ConnectionState.AUTH_INITIATED, 30);
        stateTimeouts.put(ConnectionState.AUTH_CHALLENGE_SENT, 60);
        stateTimeouts.put(ConnectionState.AUTH_RESPONSE_SENT, 60);
        stateTimeouts.put(ConnectionState.AUTH_VALIDATING, 30);
        stateTimeouts.put(ConnectionState.SESSION_CREATING, 30);
        stateTimeouts.put(ConnectionState.SESSION_REFRESHING, 60);
        stateTimeouts.put(ConnectionState.KEY_ROTATION_INITIATED, 60);
        stateTimeouts.put(ConnectionState.KEY_ROTATION_IN_PROGRESS, 120);
        stateTimeouts.put(ConnectionState.ERROR_RECOVERING, 120);
        stateTimeouts.put(ConnectionState.CLOSING, 30);
        
        
        recordTransition(null, currentState, StateEvent.RESET);
    }
    
    


    public ConnectionState getCurrentState() {
        return currentState;
    }
    
    


    public int getConnectionId() {
        return connectionId;
    }
    
    


    public boolean transition(StateEvent event) {
        stateLock.lock();
        try {
            ConnectionState newState = TRANSITION_TABLE.getOrDefault(currentState, new HashMap<>()).get(event);
            
            if (newState == null) {
                if (currentState == ConnectionState.ERROR_DETECTED) {
                    return false;
                }
                newState = ConnectionState.ERROR_DETECTED;
            }
            
            ConnectionState oldState = currentState;
            currentState = newState;
            
            
            stateVisitCounters.get(newState).incrementAndGet();
            
            
            recordTransition(oldState, newState, event);
            
            
            notifyStateChange(oldState, newState, event);
            
            return true;
        } finally {
            stateLock.unlock();
        }
    }
    
    


    public boolean forceTransition(ConnectionState newState, StateEvent event) {
        stateLock.lock();
        try {
            ConnectionState oldState = currentState;
            currentState = newState;
            
            
            stateVisitCounters.get(newState).incrementAndGet();
            
            
            recordTransition(oldState, newState, event);
            
            
            notifyStateChange(oldState, newState, event);
            
            if (newState == ConnectionState.SESSION_ACTIVE && 
                oldState == ConnectionState.IDLE) {
                recordTransition(oldState, newState, StateEvent.RESET);
            }
            
            return true;
        } finally {
            stateLock.unlock();
        }
    }
    
    


    public boolean isValidTransition(StateEvent event) {
        Map<StateEvent, ConnectionState> transitions = TRANSITION_TABLE.get(currentState);
        return transitions != null && transitions.containsKey(event);
    }
    
    


    public java.util.List<ConnectionState> getPossibleNextStates() {
        Map<StateEvent, ConnectionState> transitions = TRANSITION_TABLE.get(currentState);
        if (transitions == null) {
            return new java.util.ArrayList<>();
        }
        return new java.util.ArrayList<>(transitions.values());
    }
    
    


    public int getStateVisitCount(ConnectionState state) {
        return stateVisitCounters.getOrDefault(state, new AtomicInteger(0)).get();
    }
    
    


    public java.util.List<StateTransition> getTransitionHistory() {
        return new java.util.ArrayList<>(transitionHistory);
    }
    
    


    private void recordTransition(ConnectionState oldState, ConnectionState newState, StateEvent event) {
        StateTransition transition = new StateTransition(
            oldState,
            newState,
            event,
            System.currentTimeMillis()
        );
        
        transitionHistory.add(transition);
        
        
        if (transitionHistory.size() > MAX_TRANSITION_HISTORY) {
            transitionHistory.remove(0);
        }
    }
    
    


    public void addListener(StateChangeListener listener) {
        listeners.add(listener);
    }
    
    


    public void removeListener(StateChangeListener listener) {
        listeners.remove(listener);
    }
    
    


    private void notifyStateChange(ConnectionState oldState, ConnectionState newState, StateEvent event) {
        for (StateChangeListener listener : listeners) {
            listener.onStateChange(connectionId, oldState, newState, event);
        }
    }
    
    


    public int getStateTimeout(ConnectionState state) {
        return stateTimeouts.getOrDefault(state, 60);
    }
    
    


    public void setStateTimeout(ConnectionState state, int timeoutSeconds) {
        stateTimeouts.put(state, timeoutSeconds);
    }
    
    


    public boolean isTerminalState(ConnectionState state) {
        return state == ConnectionState.CLOSED || state == ConnectionState.TERMINATED;
    }
    
    


    public boolean isTerminal() {
        return isTerminalState(currentState);
    }
    
    


    public boolean isErrorState(ConnectionState state) {
        return state == ConnectionState.ERROR_DETECTED || state == ConnectionState.ERROR_RECOVERING;
    }
    
    


    public boolean isError() {
        return isErrorState(currentState);
    }
    
    


    public boolean isActiveState(ConnectionState state) {
        return state == ConnectionState.SESSION_ACTIVE ||
               state == ConnectionState.DATA_TRANSFER_ACTIVE ||
               state == ConnectionState.DATA_TRANSFER_READY;
    }
    
    


    public boolean isActive() {
        return isActiveState(currentState);
    }
    
    


    public void reset() {
        stateLock.lock();
        try {
            ConnectionState oldState = currentState;
            currentState = ConnectionState.IDLE;
            transitionHistory.clear();
            
            for (AtomicInteger counter : stateVisitCounters.values()) {
                counter.set(0);
            }
            
            recordTransition(oldState, currentState, StateEvent.RESET);
            notifyStateChange(oldState, currentState, StateEvent.RESET);
        } finally {
            stateLock.unlock();
        }
    }
    
    


    public static class StateTransition {
        public final ConnectionState fromState;
        public final ConnectionState toState;
        public final StateEvent event;
        public final long timestamp;
        
        public StateTransition(ConnectionState fromState, ConnectionState toState, StateEvent event, long timestamp) {
            this.fromState = fromState;
            this.toState = toState;
            this.event = event;
            this.timestamp = timestamp;
        }
    }
    
    


    public interface StateChangeListener {
        void onStateChange(int connectionId, ConnectionState oldState, ConnectionState newState, StateEvent event);
    }
}
