# 🚀 دليل نشر لوحة مسابقة FIFA 2026 على Render.com

## المتطلبات
- حساب GitHub (مجاني)
- حساب Render.com (مجاني)
- Git مُثبَّت على جهازك

---

## الخطوة 1: رفع المشروع على GitHub

```bash
# 1. افتح Terminal في مجلد المشروع
cd fifa-dashboard

# 2. أنشئ مستودع Git
git init
git add .
git commit -m "Initial commit - FIFA WC2026 Dashboard"

# 3. أنشئ مستودعاً جديداً على github.com ثم اربطه
git remote add origin https://github.com/YOUR_USERNAME/fifa-dashboard.git
git push -u origin main
```

---

## الخطوة 2: النشر على Render.com

1. اذهب إلى **https://render.com** وسجّل دخولك
2. اضغط **New → Web Service**
3. اربط حساب GitHub واختر مستودع `fifa-dashboard`
4. اضبط الإعدادات التالية:

| الحقل | القيمة |
|-------|--------|
| **Name** | `fifa-wc2026-dashboard` |
| **Runtime** | `Python 3` |
| **Build Command** | `pip install -r requirements.txt` |
| **Start Command** | `uvicorn backend.main:app --host 0.0.0.0 --port $PORT` |
| **Instance Type** | `Free` |

5. أضف **Disk** (لحفظ قاعدة البيانات):
   - اضغط **Add Disk**
   - Mount Path: `/opt/render/project/src/data`
   - Size: `1 GB`

6. أضف **Environment Variables**:

| المتغير | القيمة |
|---------|--------|
| `ADMIN_USER` | `admin` |
| `ADMIN_PASS` | كلمة مرور قوية من اختيارك |
| `DB_PATH` | `/opt/render/project/src/data/predictions.db` |

7. اضغط **Create Web Service**

⏳ انتظر 2-3 دقائق حتى يكتمل البناء.

---

## الخطوة 3: التحقق من النشر

بعد اكتمال النشر ستظهر رابط بهذا الشكل:
```
https://fifa-wc2026-dashboard.onrender.com
```

- **اللوحة الرئيسية:** `https://your-app.onrender.com/`
- **لوحة المشرف:** `https://your-app.onrender.com/admin.html`
- **API:** `https://your-app.onrender.com/api/stats`

---

## الخطوة 4: إدخال نتائج المباريات

### الطريقة الأولى: واجهة الويب (الأسهل)
1. اذهب إلى `/admin.html`
2. أدخل اسم المستخدم وكلمة المرور
3. اختر المباراة والنتيجة واضغط **تسجيل ✓**
4. النقاط تُحسَب فوراً لجميع المشاركين

### الطريقة الثانية: API مباشرة (أسرع)
```bash
curl -X POST https://your-app.onrender.com/api/admin/result \
  -u admin:YOUR_PASS \
  -H "Content-Type: application/json" \
  -d '{"round": 3, "match_name": "Brazil vs Scotland", "result": "Brazil Win"}'
```

### نتائج متعددة دفعة واحدة:
```bash
curl -X POST https://your-app.onrender.com/api/admin/result/batch \
  -u admin:YOUR_PASS \
  -H "Content-Type: application/json" \
  -d '[
    {"round": 3, "match_name": "Brazil vs Scotland", "result": "Brazil Win"},
    {"round": 3, "match_name": "Morocco vs Haiti", "result": "Morocco Win"}
  ]'
```

---

## ملاحظات مهمة

### النسخة المجانية على Render
- ⚠️ الخادم ينام بعد 15 دقيقة من عدم الاستخدام
- أول طلب بعد النوم يستغرق ~30 ثانية
- لتجنب هذا: اشترك في الخطة المدفوعة ($7/شهر) أو استخدم خدمة ping مثل https://uptimerobot.com

### تحديث اللوحة تلقائياً
اللوحة تتحدث كل **30 ثانية** تلقائياً لكل من يشاهدها.

### نسخ احتياطي لقاعدة البيانات
```bash
# تحميل نسخة من DB
curl -u admin:YOUR_PASS https://your-app.onrender.com/api/stats > backup_stats.json
```

---

## روابط مفيدة
- Render Dashboard: https://dashboard.render.com
- API Docs (Swagger): `https://your-app.onrender.com/docs`
