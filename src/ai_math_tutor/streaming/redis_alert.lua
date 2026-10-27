-- Phát cảnh báo đúng một lần cho mỗi alert_id (nguyên tử: kiểm khóa, XADD rồi SET trong cùng script).
--
-- alert_id là tất định theo (người học, kỹ năng, loại, sự kiện khởi phát), nên batch chạy lại
-- sinh lại đúng id cũ ⇒ khóa đã có ⇒ không XADD lần hai.
--
-- KEYS[1] = alert:{alert_id}, KEYS[2] = alerts:stream
-- ARGV[1] = MAXLEN xấp xỉ của stream (số nguyên ≥ 1); ARGV[2..] = các cặp trường/giá trị của mục stream.
-- Giá trị của alert:{alert_id}: 'pending:<stream id>' sau XADD; tầng Python đổi thành
-- 'produced:<stream id>' khi topic Kafka `alerts` đã nhận tin. Batch chạy lại thấy 'pending'
-- thì gửi Kafka lại (consumer khử trùng theo alert_id), thấy 'produced' thì bỏ qua.
-- Trả về: {1, giá trị} nếu vừa tạo, {0, giá trị hiện có} nếu đã tồn tại.
--
-- Thứ tự kiểm → XADD → SET: Redis không hoàn tác lệnh ghi khi script lỗi giữa chừng, nên khóa chỉ
-- được ghi SAU khi XADD thành công. XADD lỗi (ví dụ alerts:stream sai kiểu) ⇒ không ghi gì, batch
-- chạy lại thử lại từ đầu. Giá trị không đúng dạng '<pending|produced>:<id>' (ví dụ 'pending' trần
-- do bản script cũ ghi SET NX trước XADD rồi XADD lỗi) không trỏ tới mục stream nào ⇒ coi như chưa
-- có cảnh báo: XADD lại và ghi đè.

if (#ARGV - 1) < 2 or (#ARGV - 1) % 2 ~= 0 then
  return redis.error_reply('alert: cần MAXLEN và các cặp trường/giá trị')
end
local maxlen = tonumber(ARGV[1])
if maxlen == nil or maxlen ~= maxlen or maxlen < 1 or math.floor(maxlen) ~= maxlen then
  return redis.error_reply('alert: MAXLEN phải là số nguyên ≥ 1, nhận ' .. tostring(ARGV[1]))
end
local existing = redis.call('GET', KEYS[1])
if existing and (string.match(existing, '^pending:.+') or string.match(existing, '^produced:.+')) then
  return {0, existing}
end
local fields = {}
for j = 2, #ARGV do
  fields[#fields + 1] = ARGV[j]
end
local stream_id = redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[1], '*', unpack(fields))
local value = 'pending:' .. stream_id
redis.call('SET', KEYS[1], value)
return {1, value}
