local extask = {}

type taskimpl = {
	Task : any,
	Priority : number,
	Timeout : number,
	Thread : thread, 
	StartedAt : number,
	Status : string,
	Error : string?
}

local taskQueue = {}
local runningTasks = {}

local function createTask(Task, priority, timeout)
	return {
		Task = Task,
		Priority = priority or 0,
		Timeout = timeout or math.huge,
		Status = "pending",
		Thread = coroutine.create(Task),
		StartedAt = tick(),
		Error = nil,
	}
end

local function excuteTasks()
	table.sort(taskQueue, function(a, b)
		return a.Priority > b.Priority
	end)

	for _, task in ipairs(taskQueue) do
		if tick() - task.StartedAt < task.Timeout then
			if task.Status == "pending" and not runningTasks[task.Thread] then
				task.Status = "running"
				local s, e = coroutine.resume(task.Thread)
				if not s then
					task.Status = "failed"
					task.Error = e
					warn("Task failed:", e)
				else
					task.Status = "completed"
				end
			end
		else 
			extask.cancel(task.Thread)
		end
	end
end

function extask.spawn(Task, priority)
	if type(Task) == "thread" then
		table.insert(taskQueue, {
			Task = Task,
			Priority = priority or 0,
			Timeout = math.huge,
			Status = "pending",
			Thread = Task,
			StartedAt = tick(),
			Error = nil,
		})
	else
		local taskImpl = createTask(Task, priority, math.huge)
		table.insert(taskQueue, taskImpl)
	end
	excuteTasks()
end

function extask.delay(duration, Task, priority)
	local taskImpl = createTask(Task, priority, math.huge)
	delay(duration, function()
		table.insert(taskQueue, taskImpl)
		excuteTasks()
	end)
end

function extask.cancel(thread)
	for i, task in ipairs(taskQueue) do
		if task.Thread == thread then
			task.Status = "canceled"
			table.remove(taskQueue, i)
			break
		end
	end
end

function extask.defer(Task, ...)
	local taskImpl = createTask(Task, 0, math.huge)
	delay(0, function()
		table.insert(taskQueue, taskImpl)
		excuteTasks()
	end)
	return taskImpl
end

function extask.desynchronize()
	local curThread = coroutine.running()
	if curThread then
		coroutine.yield()
	else 
		warn("desynchronize can only be called inside a running coroutine")
	end
end

function extask.synchronize()
	local curThread = coroutine.running()
	if curThread then
		coroutine.yield()
	else
		warn("synchronize can only be called inside a running coroutine")
	end
end

function extask.wait(duration)
	local startTime = tick()
	while tick() - startTime < duration do
		coroutine.yield()
	end
end

function extask.parallel(tasks)
	for _, task in ipairs(tasks) do
		extask.spawn(task)
	end
end

function extask.once(task, ...)
	local executed = false
	local function wrapper(...)
		if not executed then
			task(...)
			executed = true
		end
	end
	extask.spawn(wrapper, ...)
end

function extask.after(task, afterTask)
	local function wrapper()
		task()
		afterTask()
	end
	extask.spawn(wrapper)
end


function extask.timeout(duration, Task, ...)
	local taskThread
	local timeoutThread

	taskThread = coroutine.create(function(...)
		Task(...)
	end)

	timeoutThread = coroutine.create(function()
		extask.wait(duration)
		if coroutine.status(taskThread) ~= "dead" then
			extask.cancel(taskThread)
			print("Task timed out!")
		end
	end)

	extask.spawn(taskThread, ...)
	extask.spawn(timeoutThread)
end

function extask.repeatTask(interval, Task, ...)
	local repeatThread = coroutine.create(function(...)
		while true do
			Task(...)
			extask.wait(interval)
		end
	end)
	extask.spawn(repeatThread, ...)
	return function()
		extask.cancel(repeatThread)
	end
end

function extask.onComplete(Task, callback, ...)
	local taskThread = coroutine.create(function(...)
		Task(...)
		callback()
	end)
	extask.spawn(taskThread, ...)
end

return extask
