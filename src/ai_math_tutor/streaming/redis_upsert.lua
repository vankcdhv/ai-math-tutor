-- Upsert trạng thái có điều kiện theo cặp (batch_id, state_version) (nguyên tử: Redis chạy cả script một lần).
--
-- foreachBatch của Spark chỉ bảo đảm at-least-once: batch có thể chạy lại sau khi lỗi/restart.
-- Script chỉ ghi khi bản mới MỚI HƠN bản đang lưu theo thứ tự từ điển trên (batch_id, state_version):
--   batch_id mới > batch_id cũ, hoặc cùng batch_id và state_version mới > state_version cũ.
-- - batch_id tăng đơn điệu qua mọi lần restart (Spark đọc từ checkpoint) và batch chạy lại giữ
--   nguyên id ⇒ chạy lại cùng batch (cùng batch_id, cùng version) không đổi gì.
-- - state_version có thể quay về 1 khi trạng thái người học hết hạn theo event time rồi người học
--   quay lại; bản ghi sau hết hạn luôn thuộc batch sau ⇒ batch_id lớn hơn ⇒ vẫn được ghi.
--
-- KEYS[k]: khóa của bản ghi thứ k (một khóa có thể lặp lại cho nhiều trường).
-- ARGV: các bản ghi nối tiếp nhau, mỗi bản ghi tương ứng một KEYS[k]:
--   'f', field, batch_id, version, value
--       trường JSON trong hash (state:{learner}, mỗi kỹ năng một trường); bản cũ đọc từ khóa
--       "batch_id" và "state_version" trong JSON đang lưu (không có / hỏng ⇒ coi như -1).
--   'h', batch_id, version, n, k1, v1, ..., kn, vn
--       cả hash phẳng (progress:{learner}:{problem}); bản cũ đọc từ trường 'batch_id' và
--       'state_version'. Danh sách cặp phải gồm đúng 'batch_id' = batch_id và 'state_version' = version.
-- Trả về: số bản ghi đã ghi (bản ghi bị bỏ vì không mới hơn không được đếm).
--
-- Redis không hoàn tác lệnh ghi khi script trả lỗi giữa chừng, nên bước 1 kiểm toàn bộ tham số
-- trước, bước 2 mới ghi: tham số sai ⇒ không ghi gì.

local function integer(text)
  local value = tonumber(text)
  if value == nil or value ~= value or value < 0 or math.floor(value) ~= value then
    return nil
  end
  return value
end

local function newer(batch_id, version, current_batch, current_version)
  return batch_id > current_batch or (batch_id == current_batch and version > current_version)
end

-- Bước 1: phân tích và kiểm.
local records = {}
local i = 1
for k = 1, #KEYS do
  local kind = ARGV[i]
  if kind == 'f' then
    local batch_id = integer(ARGV[i + 2])
    local version = integer(ARGV[i + 3])
    if ARGV[i + 1] == nil or batch_id == nil or version == nil or ARGV[i + 4] == nil then
      return redis.error_reply('upsert: bản ghi f thiếu field/batch_id/version/value ở vị trí ' .. i)
    end
    records[k] = {kind = 'f', field = ARGV[i + 1], batch_id = batch_id, version = version, value = ARGV[i + 4]}
    i = i + 5
  elseif kind == 'h' then
    local batch_id = integer(ARGV[i + 1])
    local version = integer(ARGV[i + 2])
    local n = integer(ARGV[i + 3])
    if batch_id == nil or version == nil or n == nil or n < 1 or (i + 3 + 2 * n) > #ARGV then
      return redis.error_reply('upsert: bản ghi h thiếu batch_id/version/số cặp ở vị trí ' .. i)
    end
    local pairs_args = {}
    local seen_batch, seen_version = false, false
    for j = 0, n - 1 do
      local name, value = ARGV[i + 4 + 2 * j], ARGV[i + 5 + 2 * j]
      if name == 'batch_id' then
        seen_batch = tonumber(value) == batch_id
      elseif name == 'state_version' then
        seen_version = tonumber(value) == version
      end
      pairs_args[#pairs_args + 1] = name
      pairs_args[#pairs_args + 1] = value
    end
    if not (seen_batch and seen_version) then
      return redis.error_reply('upsert: bản ghi h phải chứa batch_id và state_version khớp tham số ở vị trí ' .. i)
    end
    records[k] = {kind = 'h', batch_id = batch_id, version = version, pairs = pairs_args}
    i = i + 4 + 2 * n
  else
    return redis.error_reply('upsert: loại bản ghi không hợp lệ ở vị trí ' .. i .. ': ' .. tostring(kind))
  end
end
if i ~= #ARGV + 1 then
  return redis.error_reply('upsert: số tham số không khớp số khóa')
end

-- Bước 2: ghi có điều kiện.
local applied = 0
for k = 1, #KEYS do
  local key = KEYS[k]
  local record = records[k]
  local current_batch, current_version = -1, -1
  if record.kind == 'f' then
    local current = redis.call('HGET', key, record.field)
    if current then
      local ok, decoded = pcall(cjson.decode, current)
      if ok and type(decoded) == 'table' and tonumber(decoded['state_version']) then
        current_version = tonumber(decoded['state_version'])
        current_batch = tonumber(decoded['batch_id']) or -1
      end
    end
    if newer(record.batch_id, record.version, current_batch, current_version) then
      redis.call('HSET', key, record.field, record.value)
      applied = applied + 1
    end
  else
    local current = redis.call('HMGET', key, 'batch_id', 'state_version')
    current_version = tonumber(current[2]) or -1
    if current_version >= 0 then
      current_batch = tonumber(current[1]) or -1
    end
    if newer(record.batch_id, record.version, current_batch, current_version) then
      redis.call('HSET', key, unpack(record.pairs))
      applied = applied + 1
    end
  end
end
return applied
