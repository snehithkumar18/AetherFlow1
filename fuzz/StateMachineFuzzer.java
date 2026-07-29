package com.aetherflow;

import com.aetherflow.ConnectionStateMachine;
import com.aetherflow.ConnectionStateMachine.StateEvent;
import com.aetherflow.ConnectionStateMachine.ConnectionState;

public class StateMachineFuzzer {
    
    private static ConnectionStateMachine stateMachine;
    
    static {
        try {
            stateMachine = new ConnectionStateMachine(1);
        } catch (Exception e) {
            
        }
    }
    
    public static void fuzzerTestOneInput(byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }

        try {
            if (stateMachine == null) {
                return;
            }
            
            stateMachine.reset();
            
            for (int i = 0; i < data.length; i++) {
                byte b = data[i];
                StateEvent event = mapByteToEvent(b);
                
                if (event != null) {
                    stateMachine.transition(event);
                    ConnectionState currentState = stateMachine.getCurrentState();
                    stateMachine.getPossibleNextStates();
                    stateMachine.getStateVisitCount(currentState);
                    stateMachine.getTransitionHistory();
                    stateMachine.isTerminal();
                    stateMachine.isError();
                    stateMachine.isActive();
                }
            }
            
            if (data.length >= 2) {
                int stateId = data[0] & 0xFF;
                int eventId = data[1] & 0xFF;
                
                ConnectionState targetState = ConnectionState.fromId(stateId);
                StateEvent event = StateEvent.values()[eventId % StateEvent.values().length];
                
                if (targetState != null) {
                    stateMachine.forceTransition(targetState, event);
                }
            }
            
            if (data.length >= 4) {
                ThreadPoolManager tpm = new ThreadPoolManager();
                tpm.submit(ThreadPoolManager.PoolType.CPU_BOUND, () -> {
                    System.out.println("Task executed");
                });
                
                EventBus eb = new EventBus(new EventBus.EventBusConfig());
                EventBus.Event evt = new EventBus.Event("test_event", new java.util.HashMap<>());
                eb.publish(evt);
            }
            
        } catch (RuntimeException e) {
            throw e; 
        } catch (Exception e) {
            
        }
    }

    private static StateEvent mapByteToEvent(byte b) {
        StateEvent[] events = StateEvent.values();
        return events[Math.abs(b) % events.length];
    }
}
