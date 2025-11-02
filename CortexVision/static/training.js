class TrainingProgress {
    constructor() {
        this.socket = null;
        this.imageFrames = [];
        this.currentFrame = 0;
        this.progressBar = document.getElementById('training-progress');
        this.statusText = document.getElementById('status-text');
        this.trainingCanvas = document.getElementById('training-animation');
        this.ctx = this.trainingCanvas.getContext('2d');
        this.frameInterval = null;
    }

    connect() {
        this.socket = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + window.location.host + '/ws/training');
        this.socket.onopen = () => {
            // if a fallback timer exists, clear it
            try{ if(typeof this._clearFallback === 'function') this._clearFallback(); }catch(e){}
            try{ this.stopFallbackProgress(); }catch(e){}
        };
        this.socket.onmessage = (event) => {
            try{ this.stopFallbackProgress(); }catch(e){}
            this.handleMessage(event);
        };
        this.socket.onclose = () => {
            console.log('WebSocket connection closed');
            this.stopAnimation();
        };
        this.socket.onerror = (ev) => {
            console.warn('WebSocket error', ev);
            // start fallback if not already
            try{ this.startFallbackProgress(); }catch(e){}
        };
    }

    handleMessage(event) {
        const data = JSON.parse(event.data);
        
        if (data.type === 'progress') {
            this.updateProgress(data);
        } else if (data.type === 'images') {
            this.loadTrainingImages(data.images);
        } else if (data.type === 'error') {
            this.showError(data.message);
        } else if (data.type === 'complete') {
            this.handleCompletion();
        }
    }

    updateProgress(data) {
        const { processed, total, stage } = data;
        const percent = (processed / total) * 100;
        this.progressBar.style.width = `${percent}%`;
        this.statusText.textContent = `${stage}: ${processed}/${total} images`;
    }

    async loadTrainingImages(imageUrls) {
        this.imageFrames = [];
        for (const url of imageUrls) {
            try {
                const img = new Image();
                img.src = url;
                await new Promise((resolve, reject) => {
                    img.onload = resolve;
                    img.onerror = reject;
                });
                this.imageFrames.push(img);
            } catch (error) {
                console.error('Failed to load image:', error);
            }
        }
        this.startAnimation();
    }

    startAnimation() {
        if (this.frameInterval) return;
        
        this.frameInterval = setInterval(() => {
            if (this.imageFrames.length === 0) return;
            
            const img = this.imageFrames[this.currentFrame];
            
            // Clear canvas and draw image
            this.ctx.clearRect(0, 0, this.trainingCanvas.width, this.trainingCanvas.height);
            
            // Calculate dimensions to maintain aspect ratio
            const scale = Math.min(
                this.trainingCanvas.width / img.width,
                this.trainingCanvas.height / img.height
            );
            
            const width = img.width * scale;
            const height = img.height * scale;
            const x = (this.trainingCanvas.width - width) / 2;
            const y = (this.trainingCanvas.height - height) / 2;
            
            this.ctx.drawImage(img, x, y, width, height);
            
            this.currentFrame = (this.currentFrame + 1) % this.imageFrames.length;
        }, 200); // Change frame every 200ms
    }

    stopAnimation() {
        if (this.frameInterval) {
            clearInterval(this.frameInterval);
            this.frameInterval = null;
        }
    }

    showError(message) {
        this.statusText.textContent = `Error: ${message}`;
        this.statusText.classList.add('error');
    }

    handleCompletion() {
        this.statusText.textContent = 'Training complete!';
        this.progressBar.style.width = '100%';
        this.progressBar.classList.add('complete');
        setTimeout(() => {
            window.location.href = '/';
        }, 3000);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const training = new TrainingProgress();
    // try to connect to a websocket for live progress; if unavailable, fallback
    // to a local animated progress so the user sees a loader.
    try{
        training.connect();
        // if socket fails to open within a short time, start fallback
        const fallbackTimer = setTimeout(()=>{
            if(!training.socket || training.socket.readyState !== WebSocket.OPEN){
                training.startFallbackProgress();
            }
        }, 800);
        // clear fallback timer on successful connection
        training._clearFallback = () => clearTimeout(fallbackTimer);
    }catch(e){
        console.warn('WebSocket connect failed, using fallback progress', e);
        training.startFallbackProgress();
    }
});

// extend small fallback behavior onto prototype
TrainingProgress.prototype.startFallbackProgress = function(){
    // animate progress up to 85% slowly and show a status message
    if(this._fallbackInterval) return;
    let percent = 5;
    this.progressBar.style.width = `${percent}%`;
    this.statusText.textContent = 'Preparing images...';
    this._fallbackInterval = setInterval(()=>{
        percent = Math.min(85, percent + (Math.random()*4));
        this.progressBar.style.width = `${percent}%`;
        // small canvas animation to make it feel alive
        try{
            if(this.trainingCanvas && this.ctx){
                this.ctx.fillStyle = '#f6f6f6';
                this.ctx.fillRect(0,0,this.trainingCanvas.width,this.trainingCanvas.height);
                this.ctx.fillStyle = '#ddd';
                const x = Math.floor(Math.random()*(this.trainingCanvas.width-40));
                const y = Math.floor(Math.random()*(this.trainingCanvas.height-40));
                this.ctx.fillRect(x,y,40,30);
            }
        }catch(e){}
    }, 400);
};

TrainingProgress.prototype.stopFallbackProgress = function(){
    if(this._fallbackInterval){
        clearInterval(this._fallbackInterval);
        this._fallbackInterval = null;
    }
};