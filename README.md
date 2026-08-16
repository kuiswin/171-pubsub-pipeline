# Async AI Translation Pipeline (Pub/Sub × Cloud Run)

Google Cloud 実践検証シリーズ【第171弾】のソースコード実体です。

Google Cloud Pub/Sub Push 配信（OIDC JWT 認証 / Audience 整合）× Cloud Run × Gemini 3.7 Flash グローバルエンドポイントによる、急激なアクセススパイクでも破綻しない非同期 AI 翻訳パイプライン検証環境です。

## 📖 詳しい解説・チュートリアル
本リポジトリの設計思想、ローカル検証（Docker Compose / Pub/Sub Emulator）、および Google Cloud 本番デプロイ手順の詳細解説は、Qiita および技術ブログにて公開しています：

👉 **Qiita 記事一覧**: [https://qiita.com/kuis](https://qiita.com/kuis)  
👉 **Author Blog**: [https://kuis.win](https://kuis.win)

---

## 🚀 クイックスタート (ローカル検証)

```bash
# コンテナ起動 (Pub/Sub Emulator + Node.js Consumer)
docker compose up -d

# ブラウザでアクセス
open http://localhost:8080/
```

---

* 📜 **License**: MIT License (Copyright (c) 2026 kuiswin)
