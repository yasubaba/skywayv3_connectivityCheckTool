import { SkyWayContext, SkyWayRoom, SkyWayStreamFactory } from '@skyway-sdk/room';

const appIdInput = document.getElementById('app-id') as HTMLInputElement;
const secretKeyInput = document.getElementById('secret-key') as HTMLInputElement;
const roomNameInput = document.getElementById('room-name') as HTMLInputElement;
const startButton = document.getElementById('start-test') as HTMLButtonElement;
const logArea = document.getElementById('log-area') as HTMLDivElement;

function appendLog(message: string) {
  const now = new Date().toLocaleTimeString();
  logArea.textContent += `[${now}] ${message}\n`;
  logArea.scrollTop = logArea.scrollHeight;
}

startButton.onclick = async () => {
  const appId = appIdInput.value.trim();
  const secretKey = secretKeyInput.value.trim();
  const roomName = roomNameInput.value.trim();

  if (!appId || !secretKey || !roomName) {
    alert('すべての項目を入力してください。');
    return;
  }

  logArea.textContent = '';
  appendLog('=== 疎通確認テストを開始します ===');

  try {
    // ------------------------------------------------------------
    // 1. RTC-APIサーバーとの接続確認 (Publisherインスタンス)
    // ------------------------------------------------------------
    appendLog('【1/3】[Publisher] RTC-APIサーバーへ接続中 (turnPolicy: turnOnly)...');
    
    const pubContext = await SkyWayContext.CreateForDevelopment(appId, secretKey, {
      rtcConfig: { turnPolicy: 'turnOnly' },
    });
    
    appendLog('✅ [Publisher] RTC-APIサーバーの疎通確認に成功しました。');

    // ------------------------------------------------------------
    // 2. シグナリングサーバー および TURNサーバーとの接続確認
    // ------------------------------------------------------------
    appendLog('【2/3】[Publisher] シグナリングサーバーへ接続、ルームに参加中...');
    
    const pubRoom = await SkyWayRoom.FindOrCreate(pubContext, { name: roomName });
    const pubMember = await pubRoom.join({ name: 'test-publisher' });
    appendLog(`✅ [Publisher] ルームに参加しました。 Member ID: ${pubMember.id}`);

    // DataStreamのみをPublish
    appendLog('[Publisher] DataStreamを作成し、Publishします...');
    const dataStream = await SkyWayStreamFactory.createDataStream();
    const publication = await pubMember.publish(dataStream, { type: 'p2p' });
    appendLog(`✅ [Publisher] DataStreamをPublishしました。 Publication ID: ${publication.id}`);

    appendLog('--------------------------------------------------');
    
    // --- Subscriber側の処理 (別インスタンスとして実行) ---
    appendLog('【3/3】[Subscriber] 別インスタンスでRTC-APIサーバーへ接続中...');
    const subContext = await SkyWayContext.CreateForDevelopment(appId, secretKey);
    appendLog('✅ [Subscriber] RTC-APIサーバーの疎通確認に成功しました。');

    appendLog('[Subscriber] ルームに参加中...');
    const subRoom = await SkyWayRoom.FindOrCreate(subContext, { name: roomName });
    const subMember = await subRoom.join({ name: 'test-subscriber' });
    appendLog(`✅ [Subscriber] ルームに参加しました。 Member ID: ${subMember.id}`);

    // テストの重複完了出力を防ぐフラグ
    let isTestCompleted = false;

    // 購読処理とConnectionStateの監視
    const handleSubscribe = async (pub: any) => {
      if (pub.publisher.id === pubMember.id && pub.contentType === 'data') {
        appendLog(`[Subscriber] 対象のパブリケーションを検知 (ID: ${pub.id})。Subscribeを開始...`);
        
        // 1. Subscribeを実行
        const { subscription } = await subMember.subscribe(pub.id);
        appendLog(`✅ [Subscriber] Subscribe完了。 Subscription ID: ${subscription.id}`);
        
        // 状態判定の共通ロジック
        const processState = (state: string) => {
          appendLog(`🔄 [Subscriber] ConnectionState: ${state}`);
          
          if (state === 'connected' && !isTestCompleted) {
            isTestCompleted = true; // 最初に 'connected' になった時だけ実行
            appendLog('\n======================================================');
            appendLog('🎉 【疎通確認結果：すべて成功】');
            appendLog('1. RTC-APIサーバー: 正常 (Context作成完了)');
            appendLog('2. シグナリングサーバー: 正常 (ルーム管理・シグナリング確立完了)');
            appendLog('3. TURNサーバー: 正常 (turnOnlyでのP2Pデータ接続に成功)');
            appendLog('======================================================');
          }
        };

        // 2. 今後の状態遷移をキャッチするリスナーを設置
        subscription.onConnectionStateChanged.add((state) => {
          processState(state);
        });

        // 3. 【ここが重要】登録が間に合わず、すでに変化していた場合のために現在の状態を即時チェック
        
        await new Promise((resolve) => setTimeout(resolve, 5000));
        const currentState = subscription.getConnectionState();
        processState(currentState);
      }
    };

    // 既存のパブリケーションを確認
    for (const pub of subRoom.publications) {
      await handleSubscribe(pub);
    }

    // 後からパブリッシュされた場合もリッスン
    subRoom.onStreamPublished.add(async (e) => {
      await handleSubscribe(e.publication);
    });

  } catch (error) {
    appendLog(`❌ エラーが発生しました: ${error instanceof Error ? error.message : error}`);
    console.error(error);
  }
};