// Camera control and capture, with enable/disable toggle
(() => {
  const useBtn = document.getElementById('useCameraBtn');
  const captureBtn = document.getElementById('captureBtn');
  const video = document.getElementById('webcamFeed');
  const imageDataInput = document.getElementById('imageDataInput');
  const form = document.getElementById('detectForm');
  let stream = null;

  function stopCamera() {
    if (stream) {
      stream.getTracks().forEach(track => track.stop());
      stream = null;
    }
    video.style.display = 'none';
    captureBtn.style.display = 'none';
    useBtn.textContent = 'Use Camera';
    useBtn.classList.add('secondary');
  }

  async function startCamera() {
    if (stream) return;
    try {
      stream = await navigator.mediaDevices.getUserMedia({ video: true });
      video.srcObject = stream;
      video.style.display = 'block';
      captureBtn.style.display = 'inline-block';
      useBtn.textContent = 'Disable Camera';
      useBtn.classList.remove('secondary');
    } catch(err) {
      alert('Unable to access camera: ' + err.message);
    }
  }

  useBtn && useBtn.addEventListener('click', (e) => {
    e.preventDefault();
    if (stream) {
      stopCamera();
    } else {
      startCamera();
    }
  });

  captureBtn && captureBtn.addEventListener('click', (e) => {
    e.preventDefault();
    if (!stream) {
      alert('Please click "Use Camera" first to start the camera.');
      return;
    }
    const canvas = document.createElement('canvas');
    canvas.width = video.videoWidth || 640;
    canvas.height = video.videoHeight || 480;
    const ctx = canvas.getContext('2d');
    ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
    const dataUrl = canvas.toDataURL('image/jpeg', 0.9);
    if (imageDataInput) imageDataInput.value = dataUrl;
    // Submit the form with the base64 image in hidden input
    if (form) form.submit();
    // Clean up camera after capture
    stopCamera();
  });
})();
// Webcam capture helper. Expects elements with ids: webcamFeed, captureBtn, uploadInput
const videoElement = document.getElementById('webcamFeed');
const captureBtn = document.getElementById('captureBtn');
const uploadInput = document.getElementById('cameraImageInput');

async function startWebcam(){
    try{
        const stream = await navigator.mediaDevices.getUserMedia({ video: true });
        videoElement.srcObject = stream;
        videoElement.play();
    }catch(err){
        console.error('Error accessing webcam', err);
    }
}

function captureImage(){
    const canvas = document.createElement('canvas');
    canvas.width = videoElement.videoWidth || 640;
    canvas.height = videoElement.videoHeight || 480;
    const ctx = canvas.getContext('2d');
    ctx.drawImage(videoElement, 0, 0, canvas.width, canvas.height);
    canvas.toBlob(function(blob){
        // create a File and set it on the hidden file input so the form can submit it
        const file = new File([blob], 'webcam.jpg', { type: 'image/jpeg' });
        const dataTransfer = new DataTransfer();
        dataTransfer.items.add(file);
        uploadInput.files = dataTransfer.files;
        // optionally submit the parent form
        const form = document.getElementById('detectForm');
        if(form){
            form.submit();
        }
    }, 'image/jpeg', 0.92);
}

// wire events
if(videoElement){
    startWebcam();
}
if(captureBtn){
    captureBtn.addEventListener('click', function(e){
        e.preventDefault();
        captureImage();
    });
}
