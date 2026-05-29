package org.koitharu.pausingcoroutinedispatcher.internal
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Runnable
import kotlinx.coroutines.internal.SynchronizedObject
import kotlinx.coroutines.internal.synchronized
import org.koitharu.pausingcoroutinedispatcher.PausingHandle
import kotlin.concurrent.Volatile
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

internal class PausingDispatchQueue(
    initialPaused: Boolean = false,
) : AbstractCoroutineContextElement(Key), PausingHandle {

    @Volatile
    private var paused = initialPaused
    private val queue = ArrayDeque<Resumer>()
    private val sync = SynchronizedObject()

    override val isPaused: Boolean
        get() = paused

    override fun pause() {
        synchronized(sync) {
            paused = true
        }
    }

    override fun resume() {
        synchronized(sync) {
            if (paused) {
                paused = false
                dispatchAll()
            }
        }
    }

    fun enqueue(context: CoroutineContext, block: Runnable, dispatcher: CoroutineDispatcher) {
        synchronized(sync) {
            queue.addLast(Resumer(dispatcher, context, block))
        }
    }

    /**
     * Dispatch all pending tasks from the queue.
     * Must be called while holding [sync] lock.
     * Each task is dispatched to the base dispatcher so they can run in parallel.
     */
    private fun dispatchAll() {
        while (true) {
            val resumer = queue.removeFirstOrNull() ?: break
            resumer.dispatch()
        }
    }

    /**
     * Try to dispatch the next pending task if not paused.
     * Called after a task completes, to chain-dispatch the next one.
     */
    private fun tryDispatchNext() {
        synchronized(sync) {
            if (!paused) {
                val resumer = queue.removeFirstOrNull() ?: return
                resumer.dispatch()
            }
        }
    }

    override fun toString(): String {
        return "PausingDispatchQueue@${hashCode()}"
    }

    private inner class Resumer(
        private val dispatcher: CoroutineDispatcher,
        private val context: CoroutineContext,
        private val block: Runnable,
    ) : Runnable {

        override fun run() {
            block.run()
            tryDispatchNext()
        }

        fun dispatch() {
            dispatcher.dispatch(context, this)
        }
    }

    companion object Key : CoroutineContext.Key<PausingDispatchQueue>
}
