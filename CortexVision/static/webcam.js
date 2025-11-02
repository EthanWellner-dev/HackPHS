// Camera only starts when the user clicks "Use Camera".
(() => {
  const useBtn = document.getElementById('useCameraBtn');
  const captureBtn = document.getElementById('captureBtn');
  const video = document.getElementById('webcamFeed');
  const imageDataInput = document.getElementById('imageDataInput');
  const form = document.getElementById('detectForm');
  let stream = null;

  async function startCamera(){
    if(stream) return;
    try{
      stream = await navigator.mediaDevices.getUserMedia({ video: true });
      video.srcObject = stream;
      video.style.display = 'block';
      video.play && video.play();
      // show capture once camera starts
      if(captureBtn) captureBtn.style.display = 'inline-block';
      if(useBtn){ useBtn.textContent = 'Disable Camera'; useBtn.dataset.active = '1'; }
    }catch(err){
      alert('Unable to access camera: ' + err.message);
    }
  }

  function stopCamera(){
    if(stream){
      const tracks = stream.getTracks();
      for(const t of tracks) t.stop();
      stream = null;
    }
    if(video){ video.style.display = 'none'; }
    if(captureBtn) captureBtn.style.display = 'none';
    if(useBtn){ useBtn.textContent = 'Use Camera'; useBtn.dataset.active = '0'; }
  }

  useBtn && useBtn.addEventListener('click', (e)=>{
    e.preventDefault();
    // toggle
    if(useBtn.dataset && useBtn.dataset.active === '1'){
      stopCamera();
    }else{
      startCamera();
    }
  });

  // capture button is hidden until camera is active
  if(captureBtn) captureBtn.style.display = 'none';
  captureBtn && captureBtn.addEventListener('click', (e)=>{
    e.preventDefault();
    if(!stream){
      alert('Please click "Use Camera" first to start the camera.');
      return;
    }
    const canvas = document.createElement('canvas');
    canvas.width = video.videoWidth || 640;
    canvas.height = video.videoHeight || 480;
    const ctx = canvas.getContext('2d');
    ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
    const dataUrl = canvas.toDataURL('image/jpeg', 0.9);
    if(imageDataInput) imageDataInput.value = dataUrl;
    // Submit the form with the base64 image in hidden input
    if(form) form.submit();
  });

  // No automatic webcam start: the camera will only start when Use Camera is clicked.
})();
