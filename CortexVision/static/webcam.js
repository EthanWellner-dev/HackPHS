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
